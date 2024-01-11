import json
from pathlib import Path
from typing import List, Tuple
import pytest

from fixtures.benchmark_fixture import NeonBenchmarker
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
    SnapshotDir,
    last_flush_lsn_upload,
)
from fixtures.types import TenantId, TimelineId
import fixtures.pageserver.many_tenants


@pytest.fixture(scope="function")
@pytest.mark.timeout(1000)
def getpage_throughput_fixture(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
    test_snapshot_dir: SnapshotDir,
) -> fixtures.pageserver.many_tenants.SingleTimeline:

    neon_env_builder.pageserver_config_override = "max_file_descriptors=500000;page_cache_size=16384"

    def setup_template(env: NeonEnv):
        # create our template tenant
        config = {
            "gc_period": "0s",
            "checkpoint_timeout": "10 years",
            "compaction_period": "20 s",
            "compaction_threshold": 10,
            "compaction_target_size": 134217728,
            "checkpoint_distance": 268435456,
            "image_creation_threshold": 3,
        }
        template_tenant, template_timeline = env.neon_cli.create_tenant(set_default=True)
        env.pageserver.tenant_detach(template_tenant)
        env.pageserver.tenant_attach(template_tenant, config)
        # with env.endpoints.create_start("main", tenant_id=template_tenant) as ep:
        #     pg_bin.run_capture(["pgbench", "-i", "-s5", ep.connstr()])
        #     last_flush_lsn_upload(env, ep, template_tenant, template_timeline)
        ep = env.endpoints.create_start("main", tenant_id=template_tenant)
        ep.safe_psql("create table foo(b text)")
        for i in range(0, 8):
            ep.safe_psql("insert into foo(b) values ('some text')")
            last_flush_lsn_upload(env, ep, template_tenant, template_timeline)
        ep.stop_and_destroy()
        return (template_tenant, template_timeline, config)

    return fixtures.pageserver.many_tenants.single_timeline(
        neon_env_builder,
        test_snapshot_dir,
        setup_template,
        20_000,
    )


def test_getpage_throughput(
    getpage_throughput_fixture: fixtures.pageserver.many_tenants.SingleTimeline,
    zenbenchmark: NeonBenchmarker,
    pg_bin: PgBin,
):
    env = getpage_throughput_fixture.env
    ps_http = env.pageserver.http_client()

    import pdb
    pdb.set_trace()

    # run the benchmark with one client per timeline, each doing 10k requests to random keys.
    duration = "10s"
    cmd = [
        str(env.neon_binpath / "pagebench"),
        "get-page-latest-lsn",
        "--mgmt-api-endpoint",
        ps_http.base_url,
        "--page-service-connstring",
        env.pageserver.connstr(password=None),
        "--runtime",
        duration,
        # "--per-target-rate-limit", "50",
        # don't specify the targets, our fixture prepares us exactly 20k tenants,
        # and pagebench will auto-discover them
    ]
    log.info(f"command: {' '.join(cmd)}")
    basepath = pg_bin.run_capture(cmd, with_command_header=False)
    results_path = Path(basepath + ".stdout")
    log.info(f"Benchmark results at: {results_path}")

    with open(results_path, "r") as f:
        results = json.load(f)

    log.info(f"Results:\n{json.dumps(results, sort_keys=True, indent=2)}")

    env.pageserver.stop(
        immediate=True
    )  # with 20k tenants, we hit neon_local's shutdown timeout of 10 seconds

    zenbenchmark.record_pagebench_results("get-page-latest-lsn", results, duration)
