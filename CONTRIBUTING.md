# Contributing

## Development Rules

Keep changes scoped to one layer: `src/cls` for object-local Ceph operations,
`src/coordinator` for global HNSW orchestration, `src/importer` for offline base
creation, and `include/nsvu` for shared protocol changes. Update protocol and
CLS code together whenever a request or reply structure changes.

Use C++17, four-space indentation, `snake_case` functions and variables, and
descriptive `PascalCase` types. New CLS methods must be batch-oriented where
possible and must report errors through their protocol reply.

## Validation

Build with explicit `CEPH_SRC` and `HNSWLIB_INCLUDE` paths. Before large runs,
use a dedicated test cluster and a small import/update smoke test. Record the
commit, CLS binary hash, Ceph version, pool placement, dataset configuration,
and update parallelism with each result.

## Safety and Review

Never commit keyrings, passwords, private host information, datasets, compiled
artifacts, or raw results. Pool initialization is destructive and requires the
explicit confirmation flag. Pull requests should describe the experiment setup,
commands run, correctness impact, and performance changes; attach only compact
summaries or plots, not raw benchmark output.
