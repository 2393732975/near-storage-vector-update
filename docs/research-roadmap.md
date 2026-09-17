# Research Roadmap

## Baselines Already Represented

The compute-node baseline fetches candidate vectors from Ceph before computing
distances locally. The OSD-side baseline instead runs local distance scoring and
atomic adjacency patches through Ceph CLS. Together they establish that data
movement, RPC/queueing, OMAP access, patches, and global metadata contention
matter more than distance arithmetic alone.

## Next Mechanisms

1. **Graph-aware placement:** co-locate strongly connected vectors and adjacency
   state while respecting capacity and load limits; measure cross-PG fan-out.
2. **Two-level aggregation:** batch requests within an update and micro-batch
   across updates; schedule by OSD RTT, queueing, and inflight load.
3. **Metadata protocol:** move high-frequency state to shard-local metadata and
   conditionally update the global header only for global structural changes.

## Evaluation Discipline

Compare mechanisms only with identical data, pool layout, owner placement,
Ceph/CLS versions, and concurrency settings. Report throughput, tail latency,
cross-OSD fan-out, CLS calls, queueing, metadata conflicts, and data movement.
