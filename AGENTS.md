# Repository Guidelines

## Scope

This repository is the curated research codebase for near-storage HNSW update
experiments. Keep shared messages in `include/nsvu/`, OSD-side CLS logic in
`src/cls/`, global orchestration in `src/coordinator/`, and base import code in
`src/importer/`. Do not add datasets, keyrings, host configuration, generated
metrics, or compiled artifacts.

## Required Git Workflow

Every change, including documentation and configuration, requires a focused Git
commit. First run `git status --short` and confirm the branch. Preserve unrelated
working-tree changes. Implement one logical unit, then run the relevant build,
shell syntax check, or smoke test. Inspect `git diff` and `git diff --check`
before staging.

Stage explicit file paths. Verify `git diff --cached --name-status` and ensure
the staged set contains no credentials, data, binaries, logs, or raw results.
Commit with `type(scope): imperative summary`, such as
`feat(cls): batch local distance requests`. State material verification commands
and limitations in the commit body. Inspect the resulting commit before a normal
push to the intended remote and branch. Never force-push shared history.

## Experiment Safety

Pool initialization and CLS deployment modify a live Ceph test cluster. They
require explicit user authorization. Record the commit, CLS binary hash, Ceph
version, pool placement, dataset settings, and concurrency for every benchmark.
