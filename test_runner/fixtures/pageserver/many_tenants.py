import os
import shutil
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Tuple

import fixtures.pageserver.remote_storage
from fixtures import work_queue
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    SnapshotDir,
)
from fixtures.pageserver.utils import (
    wait_until_all_tenants_state,
    wait_until_tenant_state,
)
from fixtures.remote_storage import LocalFsStorage, RemoteStorageKind
from fixtures.types import TenantId, TimelineId


@dataclass
class SingleTimeline:
    env: NeonEnv


def single_timeline(
    neon_env_builder: NeonEnvBuilder,
    snapshot_dir: SnapshotDir,
    setup_template: Callable[[NeonEnv], Tuple[TenantId, TimelineId, Dict[str, Any]]],
    ncopies: int,
) -> SingleTimeline:
    """
    Create (or rehydrate from `snapshot_dir`) an env with `ncopies` copies
    of a template tenant with a single timeline.
    """

    save_snapshot = os.getenv("CI", "false") != "true"

    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    if snapshot_dir.is_initialized():
        save_snapshot = False
        env = neon_env_builder.from_repo_dir(snapshot_dir.path)
        ps_http = env.pageserver.http_client()
    else:
        if snapshot_dir.path.exists():
            shutil.rmtree(snapshot_dir.path)

        if save_snapshot and neon_env_builder.test_overlay_dir is not None:
            # Make repo_dir an overlayfs mount with lowerdir being the empty snapshot_dir.
            # When we're done filling up repo_dir, tear everything down, unmount the overlayfs, and use
            # the upperdir as the snapshot. This is equivalent to docker `FROM scratch`.
            assert not neon_env_builder.repo_dir.exists()
            assert neon_env_builder.repo_dir.parent.exists()
            snapshot_dir.path.mkdir()
            neon_env_builder.overlay_mount(
                "create-snapshot-repo-dir", snapshot_dir.path, neon_env_builder.repo_dir
            )
            neon_env_builder.config_init_force = "empty-dir-ok"

        env = neon_env_builder.init_start()

        remote_storage = env.pageserver_remote_storage
        assert isinstance(remote_storage, LocalFsStorage)

        ps_http = env.pageserver.http_client()
        # clean up the useless default tenant
        ps_http.tenant_delete(env.initial_tenant)

        log.info("invoking callback to create template tenant")
        template_tenant, template_timeline, template_config = setup_template(env)
        log.info(
            f"template tenant is template_tenant={template_tenant} template_timeline={template_timeline}"
        )

        log.info("detach template tenant form pageserver")
        env.pageserver.http_client().tenant_detach(template_tenant)
        log.info(f"duplicating template tenant {ncopies} times in S3")
        tenants = fixtures.pageserver.remote_storage.duplicate_tenant(env, template_tenant, ncopies)

        log.info("attach duplicated tenants to pageserver")
        # In theory we could just attach all the tenants, force on-demand downloads via mgmt API, and be done.
        # However, on-demand downloads are quite slow ATM.
        # => do the on-demand downloads in Python.
        assert ps_http.tenant_list() == []
        # make the attach fail after it created enough on-disk state to retry loading
        # the tenant next startup, but before it can start background loops that would start download
        ps_http.configure_failpoints(("attach-before-activate", "return"))
        env.pageserver.allowed_errors.append(
            ".*attach failed, setting tenant state to Broken: attach-before-activate.*"
        )

        def attach_broken(tenant):
            env.pageserver.tenant_attach(
                tenant,
                config=template_config.copy(),
            )
            time.sleep(0.1)
            wait_until_tenant_state(ps_http, tenant, "Broken", 10)

        work_queue.do(22, tenants, attach_broken)

        env.pageserver.stop(
            immediate=True
        )  # clears the failpoint as a side-effect; immediate to avoid hitting neon_local's timeout
        tenant_timelines = list(map(lambda tenant: (tenant, template_timeline), tenants))
        log.info("python-side on-demand download the layer files into local tenant dir")
        fixtures.pageserver.remote_storage.copy_all_remote_layer_files_to_local_tenant_dir(
            env, tenant_timelines
        )

        if save_snapshot:
            env.stop(immediate=True, ps_assert_metric_no_errors=True)
            if neon_env_builder.test_overlay_dir is None:
                log.info("take snapshot using shutil.copytree")
                shutil.copytree(env.repo_dir, snapshot_dir.path)
            else:
                log.info("take snapshot by using overlayfs upperdir")
                neon_env_builder.overlay_unmount_and_move(
                    "create-snapshot-repo-dir", snapshot_dir.path
                )
                log.info("remove empty repo_dir (previously mountpoint) for snapshot overlay_mount")
                env.repo_dir.rmdir()
                # TODO from here on, we should be able to reset / goto top where snapshot_dir.is_initialized()
                log.info("make repo_dir an overlayfs mount of the snapshot we just created")
                neon_env_builder.overlay_mount(
                    "repo-dir-after-taking-snapshot", snapshot_dir.path, env.repo_dir
                )
            snapshot_dir.set_initialized()
        else:
            log.info("skip taking snapshot")

    env.start()

    log.info("wait for all tenants to become active")
    wait_until_all_tenants_state(
        ps_http, "Active", iterations=ncopies, period=1, http_error_ok=False
    )

    # ensure all layers are resident for predictiable performance
    tenants = [info["id"] for info in ps_http.tenant_list()]
    for tenant in tenants:
        for timeline in ps_http.tenant_status(tenant)["timelines"]:
            info = ps_http.layer_map_info(tenant, timeline)
            for layer in info.historic_layers:
                assert not layer.remote

    log.info("ready")
    return SingleTimeline(env)
