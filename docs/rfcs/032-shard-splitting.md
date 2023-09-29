# Shard splitting

## Summary

This RFC describes a new pageserver API for splitting an existing tenant shard into
multiple shards, and describes how to use this API to safely increase the total
shard count of a tenant.

## Motivation

In the [sharding RFC](031-sharding-static.md), a mechanism was introduced to scale
tenants beyond the capacity of a single pageserver by breaking up the key space
into stripes, and distributing these stripes across many pageservers. However,
the shard count was defined once at tenant creation time and not varied thereafter.

In practice, the expected size of a database is rarely known at creation time, and
it is inefficient to enable sharding for very small tenants: we need to be
able to create a tenant with a small number of shards (such as 1), and later expand
when it becomes clear that the tenant has grown in size to a point where sharding
is beneficial.

### Prior art

Many distributed systems have the problem of choosing how many shards to create for
tenants that do not specify an expected size up-front. There are a couple of general
approaches:

- Write to a key space in order, and start a new shard when the highest key advances
  past some point. This doesn't work well for Neon, because we write to our key space
  in many different contiguous ranges (per relation), rather than in one contiguous
  range. To adapt to this kind of model, we would need a sharding scheme where each
  relation had its own range of shards, which would be inefficient for the common
  case of databases with many small relations.
- Monitor the system, and automatically re-shard at some size threshold. For
  example in Ceph, the [pg_autoscaler](https://github.com/ceph/ceph/blob/49c27499af4ee9a90f69fcc6bf3597999d6efc7b/src/pybind/mgr/pg_autoscaler/module.py)
  component monitors the size of each RADOS Pool, and adjusts the number of Placement
  Groups (Ceph's shard equivalent).

## Requirements

- A configurable capacity limit per-shard is enforced.
- Changes in shard count do not interrupt service beyond requiring postgres
  to reconnect (i.e. milliseconds).
- Human being does not have to choose shard count

## Non Goals

- Shard splitting is always a tenant-global operation: we will not enable splitting
  one shard while leaving others intact.
- The inverse operation (shard merging) is not described in this RFC. This is a lower
  priority than splitting, because databases grow more often than they shrink, and
  a database with many shards will still work properly if the stored data shrinks, just
  with slightly more overhead (e.g. redundant WAL replication)
- Shard splitting is only initiated based on capacity bounds, not load. Splitting
  a tenant based on load will make sense for some medium-capacity, high-load workloads,
  but is more complex to reason about and likely is not desirable until we have
  shard merging to reduce the shard count again if the database becomes less busy.

## Impacted Components

pageserver, sharding service

## Terminology

**Parent** shards are the shards that exist before a split. **Child** shards are
the new shards created during a split.

**Shard** is synonymous with _tenant shard_.

**Shard Index** is the 2-tuple of shard number and shard count, written in
paths as {:02x}{:02x}, e.g. `0001`.

## Background

In the implementation section, a couple of existing aspects of sharding are important
to remember:

- Shard identifiers contain the shard number and count, so that "shard 0 of 1" (`0001`) is
  a distinct shard from "shard 0 of 2" (`0002`). This is the case in key paths, local
  storage paths, and remote index metadata.
- Remote layer file paths contain the shard index of the shard that created them, and
  remote indices contain the same index to enable building the layer file path. A shard's
  index may reference layers that were created by another shard.
- Local tenant shard directories include the shard index. All layers downloaded by
  a tenant shard are stored in this shard-prefixed path, even if those layers were
  initially created by another shard: tenant shards do read and write one anothers'
  paths.
- The `Tenant` pageserver type represents one tenant _shard_, not the whole tenant.
  This is for historical reasons and will be cleaned up in future, but the existing
  name is used here to help comprehension when reading code.

## Implementation

### Pageserver Split API

The pageserver split API operates on one tenant shard, on one pageserver. External
coordination is required to use it safely, this is described in the later
'Split procedure' section.

#### Preparation

First identify the shard indices for the new child shards. These are deterministic,
calculated from the parent shard's index, and the number of children being created (this
is an input to the API, and validated to be a power of two). In a trivial example, splitting
0001 in two always results in 0002 and 0102.

Child shard indices are chosen such that the childrens' parts of the keyspace will
be subsets of the parent's parts of the keyspace.

#### Step 1: write new remote indices

In remote storage, splitting is very simple: we may just write new index_part.json
objects for each child shard, containing exactly the same layers as the parent shard.

The children will have more data than they need, but this avoids any exhausive
re-writing or copying of layer files.

The index key path includes a generation number: the parent shard's current
attached generation number will also be used for the child shards' indices. This
makes the operation safely retryable: if everything crashes and restarts, we may
call the split API again on the parent shard, and the result will be some new remote
indices for the child shards, under a higher generation number.

#### Step 2: start new `Tenant` objects

A new `Tenant` object may be instantiated for each child shard, while the parent
shard still exists. When calling the tenant_spawn function for this object,
the remote index from step 1 will be read, and the child shard will start
to ingest WAL to catch up from whatever was in the remote storage at step 1.

We now wait for child shards' WAL ingestion to catch up with the parent shard,
so that we can safely tear down the parent shard without risking an availability
gap to clients reading recent LSNs.

#### Step 3: tear down parent `Tenant` object

Once child shards are running and have caught up with WAL ingest, we no longer
need the parent shard. Note that clients may still be using it -- when we
shut it down, any page_service handlers will also shut down, causing clients
to disconnect. When the client reconnects, it will re-lookup the tenant,
and hit the child shard instead of the parent.

Note that at this stage the page service client has not yet been notified of
any split. In the trivial single split example:

- Shard 0001 is gone: Tenant object torn down
- Shards 0002 and 0102 are running on the same pageserver where Shard 0001 used to live.
- Clients will continue to connect to that server thinking that shard 0001 is there,
  and all requests will work, because any key that was in shard 0001 is definitely
  available in either shard 0002 or shard 0102.
- Eventually, the sharding service (not the pageserver) will decide to migrate
  some child shards away: at that point it will do a live migration, ensuring
  that the client has an updated configuration before it detaches anything
  from the original server.

#### Complete

When we send a 200 response to the split request, we are promising the caller:

- That the child shards are persistent in remote storage
- That the parent shard has been shut down

This enables the caller to proceed with the overall shard split operation, which
may involve other shards on other pageservers.

### Split procedure

Splitting a tenant requires calling the pageserver split API, and tracking
enough state to ensure recovery + completion in the event of any component (pageserver
or sharding service) crashing (or request timing out) during the split.

Phase 1: call the split API on all existing shards. Ensure that the resulting
child shards are pinned to their pageservers until _all_ the split calls are done.
This pinning may be implemented as a "split bit" on the tenant shards, that
blocks any migrations, and also acts as a sign that if we restart, we must go
through some recovery steps to resume the split.

Phase 2: Once all the split calls are done, we may unpin the child shards (clear
the split bit). The split is now complete: subsequent steps are just migrations,
not strictly part of the split.

Phase 3: Try to schedule new pageserver locations for the child shards, using
a soft anti-affinity constraint to place shards from the same tenant onto different
pageservers.

Updating computes about the new shard count is not necessary until we migrate
something. At that point, the live migration process should take care of blocking
migrations until

### Recovering from failures

TODO: define pageserver behavior on restart if local storage contains some child
shards and some parents. Start them all and let the sharding service figure it out?

TODO: define shard split retry: when sharding service restarts and sees tenant
shards with the split bit set, what happens?

#### Pageserver request failures

The split request handler will implement idempotency: if the [`Tenant`] requested to split
doesn't exist, we will check for the would-be child shards, and if they already exist,
we consider the request complete.

If a request is retried while the original request is still underway, then the split
request handler will notice an InProgress marker in TenantManager, and return 503
to encourage the client to backoff/retry. This is the same as the general pageserver
API handling for calls that try to act on an InProgress shard.

#### Compute start/restart during a split

If a compute starts up during split, it will be configured with the old sharding
configuration. This will work for reads irrespective of the progress of the split
as long as no child hards have been migrated away from their original location, and
this is guaranteed in the split procedure (see earlier section).

#### Rolling back a split

Since we do not immediately delete any parent shard content, it remains possible to
attach the parent shard. A simple "undo" of a shard split may be done by the shard
service dropping its state for child shards, recreating its state for the parent
shard, and then attaching it (calculate generation as max() of child shard generations).

The parent shard would have to replay any WAL writes since the split, but would
otherwise be unperturbed and have no memory of the split having happened. Child
shard paths in remote storage would remain as garbage to be cleaned up later.

No specific pageserver API is needed to rollback a split: this may be implemented
entirely in the sharding service using the /v1/tenant/{}/location_config API.

Rolling back a split should be considered an exceptional operation, only triggered
manually in response to a bug. For example, if we encountered some critical bug in
the sharding logic that resulted in a tenant becoming unreadable, or reading garbage
after a split, we would roll back the split. This mechanism is most important in the
period shortly after sharding is deployed to production, so it should be included in
the initial implementation.

### Conditions to trigger a split

The pageserver will expose a new API for reporting on shards that are candidates
for split: this will return a top-N report of the largest tenant shards by
physical size (remote size). This should exclude any tenants that are already
at the maximum configured shard count.

The sharding service will poll that API across all pageservers it manages at some appropriate interval (e.g. 60 seconds).

When an oversized shard is seen, this makes the tenant a candidate for splitting.

## Optimizations

### Flush parent shard to remote storage during split

Any data that is in WAL but not remote storage at time of split will need
to be replayed by child shards when they start for the first time. To minimize
this work, we may flush the parent shard to remote storage before writing the
remote indices for child shards.

It is important that this flush is subject to some time bounds: we may be splitting
in response to a surge of write ingest, so it may be time-critical to split. A
few seconds to flush latest data should be sufficient to optimize common cases without
running the risk of holding up a split for a harmful length of time when a parent
shard is being written heavily. If the flush doesn't complete in time, we may proceed
to shut down the parent shard and carry on with the split.

### Hard linking parent layers into child shard directories

Before we start the Tenant objects for child shards, we may pre-populate their
local storage directories with hard links to the layer files already present
in the parent shard's local directory. When the child shard starts and downloads
its remote index, it will find all those layer files already present on local disk.

This avoids wasting download capacity and makes splitting faster, but more importantly
it avoids taking up a factor of N more disk space when splitting 1 shard into N.

This mechanism will work well in typical flows where shards are migrated away
promptly after a split, but for the general case including what happens when
layers are evicted and re-downloaded after a split, see the 'Proactive compaction'
section below.

### Filtering during compaction

Compaction, especially image layer generation, should skip any keys that are
present in a shard's layer files, but do not match the shard's ShardIdentity's
is_key_local() check. This avoids carrying around data for longer than necessary
in post-split compactions.

This was already implemented in https://github.com/neondatabase/neon/pull/6246

### Proactive compaction

In remote storage, there is little reason to rewrite any data on a shard split:
all the children can reference parent layers via the very cheap write of the child
index_part.json.

In local storage, things are more nuanced. During the initial split there is no
capacity cost to duplicating parent layers, if we implement the hard linking
optimization described above. However, as soon as any layers are evicted from
local disk and re-downloaded, the downloaded layers will not be hard-links any more:
they'll have real capacity footprint. That isn't a problem if we migrate child shards
away from the parent node swiftly, but it risks a significant over-use of local disk
space if we do not.

For example, if we did an 8-way split of a shard, and then _didn't_ migrate 7 of
the shards elsewhere, then churned all the layers in all the shards via eviction,
then we would blow up the storage capacity used on the node by 8x. If we're splitting
a 100GB shard, that could take the pageserver to the point of exhausting disk space.

To avoid this scenario, we could implement a special compaction mode where we just
read historic layers, drop unwanted keys, and write back the layer file. This
is pretty expensive, but useful if we have split a large shard and are not going to
migrate the child shards away.

The conditions for triggering such a compaction are:

- A) eviction plus time: if a child shard
  has existed for more than a time threshold, and has been requested to perform at least one eviction, then it becomes urgent for this child shard to execute a proactive compaction to reduce its storage footprint, at the cost of I/O load.
- B) resident size plus time: we may inspect the resident layers and calculate how
  many of them include the overhead of storing pre-split keys. After some time
  threshold (different to the one in case A) we still have such layers occupying
  local disk space, then we should proactively compact them.

TODO: maybe we should also proactively compact before migration? Avoid the new
location having to download parent shard garbage. Then we can simplify the rules to
compacting before the migration, or if we don't migrate then compact after some
time threshold.

### Cleaning up parent-shard layers

It is functionally harmless to leave parent shard layers in remote storage indefinitely.
They would be cleaned up in the event of the tenant's deletion.

As an optimization to avoid leaking remote storage capacity (which costs money), we may
lazily clean up parent shard layers once no child shards reference them.

This may be done _very_ lazily: e.g. check every PITR interval. The cleanup procedure is:

- list all the key prefixes beginning with the tenant ID, and select those shard prefixes
  which do not belong to the most-recently-split set of shards (_ancestral shards_, i.e. shard*count < max(shard_count) over all shards), and those shard prefixes which do belong to the current shard_count set (\_current shards*)
- If there are no _ancestral shard_ prefixes found, we have nothing to clean up and
  may drop out now.
- find the latest-generation index for each _current shard_, read all and accumulate the set of layers belonging to ancestral shards referenced by these indices.
- for all ancestral shards, list objects in the prefix and delete any layer which was not
  referenced by a current shard.

If this cleanup is scheduled for 1-2 PITR periods after the split, there is a good chance that child shards will have written their own image layers covering the whole keyspace, such that all parent shard layers will be deletable.

The cleanup may be done by the scrubber (external process), or we may choose to have
the zeroth shard in the latest generation do the work -- there is no obstacle to one shard
reading the other shard's indices at runtime, and we do not require visibility of the
latest index writes.

Cleanup should be artificially delayed by some period (for example 24 hours) to ensure
that we retain the option to roll back a split in case of bugs.

## FAQ/Alternatives

### What should the thresholds be set to?

Shard size limit: the pre-sharding default capacity quota for databases was 200GiB, so this could be a starting point for the per-shard size limit.

Max shard count:

- The safekeeper overhead to sharding is currently O(N) network bandwidth because
  the un-filtered WAL is sent to all shards. To avoid this growing out of control,
  a limit of 8 shards should be temporarily imposed until WAL filtering is implemented
  on the safekeeper.
- there is also little benefit to increasing the shard count beyond the number
  of pageservers in a region.

### Is it worth just rewriting all the data during a split to simplify reasoning about space?
