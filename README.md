# Near-Storage Vector Update

Research prototype for studying efficient online HNSW updates over Ceph RADOS.
It compares a conventional compute-node distance path with OSD-side Ceph CLS
operator offload, then profiles the remaining update bottlenecks.

## Scope

The coordinator keeps global HNSW search control. The CLS module executes
object-local vector reads, distance scoring, label lookups, and atomic adjacency
patches. An update marks the prior vector stale and reinserts its replacement;
it is not an in-place overwrite.

This repository is an experimental prototype, not a production vector database.
The current baseline is designed to measure RPC/queueing, OMAP, patch, and
global-metadata costs before implementing locality-aware placement, request
aggregation, and shard-local metadata protocols.

## Layout

```text
include/nsvu/       CLS/coordinator wire protocol and object naming
src/cls/            Ceph object-class implementation
src/coordinator/    Global HNSW build and update coordinator
src/importer/       Local HNSW base build and Ceph import path
scripts/            Explicit, safety-gated experiment helpers
config/             Dataset configuration examples
docs/               Experiment methodology and proposal material
results/            Local-only generated metrics
```

## Prerequisites

- A Ceph development tree and matching librados/CLS headers.
- A compatible `hnswlib` include directory.
- A test Ceph cluster. Supply access through standard Ceph configuration or
  `--keyring /path/to/keyring`; never commit credentials.

Set paths explicitly, then build:

```bash
export CEPH_SRC=/path/to/ceph/src
export HNSWLIB_INCLUDE=/path/to/hnswlib/include
make
```

See `config/datasets.example.json` for input conventions and `docs/` for the
experimental rationale. Pool initialization and CLS deployment change cluster
state and deliberately are not automated by the default build.

## Reproducibility

Record the Git commit, CLS binary hash, Ceph version, pool size/PG count, actual
`ceph pg map` placement, dataset options, and update parallelism for every run.
Experiment pools using replica size 1 are for controlled research only.
