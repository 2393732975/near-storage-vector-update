#!/usr/bin/env python3
"""Unattended first batch: high-recall graph qualification and import speed trials.

This is preparation/qualification, not the 24-case formal baseline. It never
deploys CLS or enables frontier/executor optimizations. Failure leaves evidence.
"""

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import signal
import statistics
import subprocess
import sys
import time

FSID = "c83da116-1e8b-11f1-933d-cd48aa44d216"
DATASETS = ("gist1m", "text2image10m", "deep100m", "sift100m")


def utc():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for data in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            h.update(data)
    return h.hexdigest()


def read_json(path):
    return json.loads(path.read_text())


class Suite:
    def __init__(self, args):
        self.args = args
        self.repo = Path(__file__).resolve().parents[1]
        self.root = args.run_root.resolve()
        if self.root.exists() and any(self.root.iterdir()):
            raise RuntimeError("RUN_ROOT must be new or empty")
        self.root.mkdir(parents=True, exist_ok=True)
        self.started = time.monotonic()
        self.current = None
        self.steps = []
        self.allowed_health = set()
        self.env = dict(os.environ, CEPH_KEYRING=str(args.keyring),
                        EXPECTED_CEPH_FSID=FSID, OWNERS="5", META_POOL="nsvu_meta",
                        OWNER_POOL_PREFIX="nsvu_owner_", CEPH_CMD="/usr/bin/ceph",
                        RADOS_CMD="/usr/bin/rados", CEPH_USE_SUDO="false")
        self.lock = (self.repo / "results/.high-recall-experiment.lock").open("w")
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.lock.write(str(os.getpid()) + "\n")
        self.lock.flush()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        for name in ("nsvu-base-importer", "nsvu-update-coordinator", "nsvu-index-checker",
                     "libcls_hnsw_global.so"):
            shutil.copy2(self.repo / "build" / name, self.bin / name)
        for name in ("baseline-quality.py", "index-content-digest.py",
                     "initialize-experiment-pools.sh", "cleanup-experiment-pools.sh"):
            shutil.copy2(self.repo / "scripts" / name, self.bin / name)
        self.status("starting")

    def status(self, state, **extra):
        doc = dict(state=state, updated_utc=utc(), pid=os.getpid(),
                   elapsed_seconds=round(time.monotonic() - self.started, 1),
                   stage="preparation_and_qualification_only", steps=self.steps, **extra)
        write_json(self.root / "status.json", doc)

    def resource_gate(self):
        if time.monotonic() - self.started > self.args.max_hours * 3600:
            raise RuntimeError("first-batch wall-time budget reached")
        if shutil.disk_usage(str(self.root)).free < self.args.min_free_gib * 2**30:
            raise RuntimeError("local free-space reserve reached")
        size = 0
        for path in self.root.rglob("*"):
            try:
                if path.is_file():
                    size += path.stat().st_size
            except FileNotFoundError:
                pass  # Child progress files may be atomically replaced.
        if size > self.args.max_artifact_gib * 2**30:
            raise RuntimeError("new artifact-space budget reached")
        if self.current:
            try:
                text = Path("/proc/%d/status" % self.current.pid).read_text()
                rss = next(int(x.split()[1]) for x in text.splitlines() if x.startswith("VmRSS:"))
                if rss * 1024 > 750 * 2**30:
                    raise RuntimeError("child RSS budget reached")
            except (FileNotFoundError, StopIteration):
                pass

    def run(self, label, command, timeout=24 * 3600, allowed=(0,)):
        self.resource_gate()
        index = len(self.steps)
        log = self.root / "logs" / ("%03d-%s.log" % (index, label))
        log.parent.mkdir(exist_ok=True)
        step = dict(label=label, started_utc=utc(), command=[str(x) for x in command],
                    log=str(log.relative_to(self.root)), state="running")
        self.steps.append(step)
        self.status("running", current_step=label)
        print("[%s] START %s" % (utc(), label), flush=True)
        t0 = time.monotonic()
        last_cluster_sample = t0
        try:
            with log.open("w") as output:
                self.current = subprocess.Popen([str(x) for x in command], cwd=str(self.repo),
                                                env=self.env, stdout=output,
                                                stderr=subprocess.STDOUT, start_new_session=True)
                while self.current.poll() is None:
                    if time.monotonic() - t0 > timeout:
                        raise RuntimeError("step timeout: " + label)
                    self.resource_gate()
                    if time.monotonic() - last_cluster_sample >= 60 and not (
                            label.endswith("-reset") or label.startswith("cleanup")):
                        self.cluster_gate()
                        sample = dict(utc=utc(), step=label,
                                      meminfo=Path("/proc/meminfo").read_text(),
                                      network=Path("/proc/net/dev").read_text())
                        with (self.root / "system-samples.jsonl").open("a") as samples:
                            samples.write(json.dumps(sample) + "\n")
                        last_cluster_sample = time.monotonic()
                    self.status("running", current_step=label)
                    try:
                        self.current.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        pass
                result = self.current.returncode
            step.update(state="completed" if result in allowed else "failed", exit_code=result,
                        seconds=round(time.monotonic() - t0, 3), finished_utc=utc())
            if result not in allowed:
                raise RuntimeError("step %s returned %d; see %s" % (label, result, log))
        except BaseException:
            if self.current and self.current.poll() is None:
                os.killpg(self.current.pid, signal.SIGTERM)
                try:
                    self.current.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    os.killpg(self.current.pid, signal.SIGKILL)
                    self.current.wait()
            step.update(state="failed", finished_utc=utc())
            raise
        finally:
            self.current = None
        self.status("running")
        print("[%s] DONE %s (%ss)" % (utc(), label, step["seconds"]), flush=True)

    def ceph_json(self, *command):
        output = subprocess.check_output(["ceph", "--keyring", str(self.args.keyring),
                                          *command, "--format", "json"], timeout=45)
        return json.loads(output)

    def cluster_gate(self, initial=False):
        fsid = subprocess.check_output(["ceph", "--keyring", str(self.args.keyring), "fsid"],
                                       timeout=30, text=True).strip()
        if fsid != FSID:
            raise RuntimeError("FSID mismatch")
        doc = self.ceph_json("status")
        osds = doc.get("osdmap", {}).get("osdmap", doc.get("osdmap", {}))
        if any(osds.get(key) != 5 for key in ("num_osds", "num_up_osds", "num_in_osds")):
            raise RuntimeError("OSDs must be 5/5 up/in")
        pg = doc["pgmap"]
        states = pg.get("pgs_by_state", [])
        if (not states or pg.get("num_pgs", 0) <= 0 or
                sum(x.get("count", 0) for x in states) != pg["num_pgs"] or
                any(x["state_name"] != "active+clean" for x in states)):
            raise RuntimeError("PGs are not all active+clean")
        checks = set(doc.get("health", {}).get("checks", {}))
        module = doc.get("health", {}).get("checks", {}).get("MGR_MODULE_ERROR", {})
        if module and module.get("summary", {}).get("message", "") != "Module 'devicehealth' has failed: disk I/O error":
            raise RuntimeError("unrecognized or expanded manager module error")
        if initial:
            if checks - {"MGR_MODULE_ERROR"}:
                raise RuntimeError("unexpected initial health checks: " + str(checks))
            self.allowed_health = checks | {"POOL_NO_REDUNDANCY", "LARGE_OMAP_OBJECTS"}
        elif checks - self.allowed_health:
            raise RuntimeError("new health checks: " + str(checks - self.allowed_health))
        utilization = self.ceph_json("osd", "df")
        if {x.get("id") for x in utilization.get("nodes", [])} != set(range(5)):
            raise RuntimeError("incomplete OSD capacity report")
        if any(float(x.get("utilization", 0)) >= 80 for x in utilization.get("nodes", [])):
            raise RuntimeError("OSD capacity threshold reached")
        write_json(self.root / "cluster-latest.json", doc)
        if initial:
            write_json(self.root / "cluster-initial.json", doc)
            write_json(self.root / "daemon-versions.json", self.ceph_json("versions"))

    def reset(self, label):
        self.cluster_gate()
        self.run(label + "-reset", ["bash", self.bin / "initialize-experiment-pools.sh", "--confirm-reset"], 600)
        deadline = time.monotonic() + 300
        while True:
            try:
                self.cluster_gate()
                return
            except RuntimeError as error:
                if "PGs" not in str(error) or time.monotonic() >= deadline:
                    raise
                time.sleep(5)

    def prepare(self, dataset):
        output = self.root / "splits" / dataset
        self.run(dataset + "-prepare", [self.args.python, self.bin / "baseline-quality.py", "prepare",
                  "--dataset", dataset, "--dataset-root", self.args.dataset_root,
                  "--output-dir", output])
        manifest = output / "split-manifest.json"
        self.run(dataset + "-verify-gt", [self.args.python, self.bin / "baseline-quality.py", "verify-gt",
                  "--manifest", manifest, "--sample-queries", "2", "--split", "calibration",
                  "--output", output / "gt-verification.json"])
        return manifest, read_json(manifest)

    def importer_args(self, manifest, m, efc):
        cfg, base = manifest["config"], manifest["base"]
        return [self.bin / "nsvu-base-importer", "--input", base["path"], "--input-format", base["format"],
                "--num-vectors", cfg["count"], "--dim", cfg["dim"], "--vector-kind", cfg["kind"],
                "--metric", cfg["metric"], "--threads", cfg["threads"], "--M", m, "--ef", efc,
                "--points-per-object", cfg["points_per_object"], "--dataset-sha256", base["sha256"]]

    def qualify(self, dataset, manifest):
        parent = self.root / "graphs" / dataset
        parent.mkdir(parents=True)
        cfg = manifest["config"]
        for m, efc in ((16, 200), (32, 200), (32, 400)):
            folder = parent / ("m%d-efc%d" % (m, efc))
            folder.mkdir()
            cache = folder / "base.hnsw"
            for width in (64, 128, 256, 512):
                quality = folder / ("calibration-ef%d.json" % width)
                split = manifest["splits"]["calibration"]
                cmd = self.importer_args(manifest, m, efc) + [
                    "--build-only", "--index-cache", cache,
                    "--index-cache-mode", "load" if cache.exists() else "build",
                    "--query-input", split["vectors"], "--query-count", split["count"],
                    "--ground-truth", split["ground_truth"], "--query-ef", width,
                    "--recall-k", 10, "--local-recall-out", quality,
                    "--metrics-out", folder / ("build-ef%d.json" % width),
                    "--progress-out", folder / "progress.json"]
                self.run("%s-m%d-efc%d-ef%d" % (dataset, m, efc, width), cmd)
                result = read_json(quality)
                value = result["recall_at_k"]
                if (not math.isfinite(value) or result["evaluated_queries"] != split["count"] or
                        result["denominator"] != split["count"] * 10):
                    raise RuntimeError("incomplete or invalid local recall result")
                if value < .92:
                    continue
                split = manifest["splits"]["validation"]
                validation = folder / "validation.json"
                self.run(dataset + "-local-validation", self.importer_args(manifest, m, efc) + [
                    "--build-only", "--index-cache", cache, "--index-cache-mode", "load",
                    "--query-input", split["vectors"], "--query-count", split["count"],
                    "--ground-truth", split["ground_truth"], "--query-ef", width,
                    "--local-recall-out", validation, "--recall-k", 10])
                result = read_json(validation)
                value = result["recall_at_k"]
                if (not math.isfinite(value) or result["evaluated_queries"] != split["count"] or
                        result["denominator"] != split["count"] * 10):
                    raise RuntimeError("incomplete or invalid held-out recall result")
                if value < .90:
                    raise RuntimeError(dataset + ": frozen candidate failed held-out local quality; no retuning on validation")
                selected = dict(M=m, ef_construction=efc, ef_update=width, ef_query=width,
                                cache=str(cache), cache_sha256=sha(cache),
                                local_quality_only=True,
                                remote_and_dynamic_quality="not_yet_certified")
                write_json(parent / "selected.json", selected)
                return selected
        raise RuntimeError(dataset + ": bounded candidate grid did not reach .92 calibration recall")

    def import_case(self, dataset, manifest, selected, workers, batch, repetition, digest):
        folder = self.root / "import-trials" / dataset / ("rep-%d-w%d-b%d" % (repetition, workers, batch))
        folder.mkdir(parents=True)
        label = "%s-import-r%d-w%d-b%d" % (dataset, repetition, workers, batch)
        if sha(Path(selected["cache"])) != selected["cache_sha256"]:
            raise RuntimeError("frozen cache content changed before import")
        self.reset(label)
        self.run(label, self.importer_args(manifest, selected["M"], selected["ef_construction"]) + [
            "--keyring", self.args.keyring, "--index-cache", selected["cache"], "--index-cache-mode", "load",
            "--import-workers", workers, "--omap-batch", batch, "--omap-batch-bytes", 8 * 1024 * 1024,
            "--metrics-out", folder / "import.json", "--progress-out", folder / "progress.json"])
        cfg = manifest["config"]
        self.run(label + "-checker", [self.bin / "nsvu-index-checker", "--keyring", self.args.keyring,
                 "--owners", 5, "--points-per-object", cfg["points_per_object"], "--batch-size", 8192,
                 "--reference-input", manifest["base"]["path"], "--reference-input-format", manifest["base"]["format"],
                 "--reference-count", cfg["count"], "--semantic-samples", 256,
                 "--semantic-min-edge-win-rate", .60, "--output", folder / "checker.json"])
        checked = read_json(folder / "checker.json")
        if checked["status"] != "pass" or checked["errors"] or checked["warnings"]:
            raise RuntimeError(label + ": checker did not pass without errors/warnings")
        if digest:
            self.run(label + "-digest", ["/usr/bin/python3", self.bin / "index-content-digest.py",
                     "--keyring", self.args.keyring, "--expected-fsid", FSID,
                     "--output", folder / "content-digest.json"])
        self.cluster_gate()
        return folder

    def import_trials(self, dataset, manifest, selected):
        cases, expected = [], None
        # Same frozen graph in every case. Three alternating rounds of GIST;
        # one large-data pair validates scaling without a full Cartesian scan.
        if dataset == "gist1m":
            configurations, repetitions = [(1, 2048), (2, 2048), (5, 2048), (5, 8192)], 3
        elif dataset == "deep100m":
            prior = read_json(self.root / "import-trials/gist1m/selected-settings.json")
            configurations = list(dict.fromkeys([(1, 2048), (prior["workers"], prior["omap_batch"])]))
            repetitions = 1
        else:
            return
        for repetition in range(1, repetitions + 1):
            order = configurations if repetition % 2 else list(reversed(configurations))
            for workers, batch in order:
                folder = self.import_case(dataset, manifest, selected, workers, batch, repetition,
                                          digest=(dataset == "gist1m"))
                if dataset == "gist1m":
                    value = read_json(folder / "content-digest.json")["logical_sha256"]
                    if expected is not None and value != expected:
                        raise RuntimeError("parallel import changed persisted logical content")
                    expected = value
                cases.append(dict(workers=workers, omap_batch=batch, repetition=repetition,
                                  metrics=read_json(folder / "import.json"), path=str(folder)))
                write_json(self.root / "import-trials" / dataset / "summary.json", cases)
        options = {(x["workers"], x["omap_batch"]) for x in cases}
        fastest = min(options, key=lambda item: statistics.median(
            x["metrics"]["total_seconds"] for x in cases if (x["workers"], x["omap_batch"]) == item))
        write_json(self.root / "import-trials" / dataset / "selected-settings.json",
                   dict(workers=fastest[0], omap_batch=fastest[1],
                        selection="lowest observed median; no guaranteed speedup"))

    def execute(self):
        self.cluster_gate(initial=True)
        versions = {p.name: sha(p) for p in self.bin.iterdir()}
        write_json(self.root / "manifest.json", dict(
            started_utc=utc(), source_commit=subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=str(self.repo), text=True).strip(),
            expected_fsid=FSID, datasets=list(self.args.datasets), binaries=versions,
            scope="offline quality qualification and import speed trials, not formal baseline",
            frontier_window=1, executor_enabled=False, max_hours=self.args.max_hours,
            deployed_cls_verification="local osd.2 hash matches; remote hashes not yet verified"))
        for dataset in self.args.datasets:
            _, manifest = self.prepare(dataset)
            selected = self.qualify(dataset, manifest)
            self.import_trials(dataset, manifest, selected)
        # Pool cleanup is restricted to the same six experiment pools. Code 3
        # means cleanup succeeded and the pre-existing health warning remains.
        self.run("cleanup-dry-run", ["bash", self.bin / "cleanup-experiment-pools.sh"], 900)
        self.run("cleanup", ["bash", self.bin / "cleanup-experiment-pools.sh", "--confirm-cleanup"],
                 900, allowed=(0, 3))
        self.cluster_gate()
        self.status("first_batch_complete", next_step=(
            "Review import speed/quality results, verify remote deployed CLS hashes, "
            "then certify remote dynamic recall and start formal paired runs. "
            "Local HNSW recall alone is not the high-recall baseline result."))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confirm-reset", action="store_true")
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--dataset-root", type=Path, default=Path("/home/hhf/datasets"))
    parser.add_argument("--keyring", type=Path, default=Path("/home/hhf/study/ceph.client.admin.keyring"))
    parser.add_argument("--python", default="/tmp/nsvu-osd-plot-venv/bin/python")
    parser.add_argument("--datasets", nargs="+", choices=DATASETS, default=list(DATASETS))
    parser.add_argument("--max-hours", type=float, default=36)
    parser.add_argument("--min-free-gib", type=float, default=250)
    parser.add_argument("--max-artifact-gib", type=float, default=700)
    args = parser.parse_args()
    if not args.confirm_reset:
        parser.error("--confirm-reset is required for authorized experiment-pool resets")
    if any(x <= 0 for x in (args.max_hours, args.min_free_gib, args.max_artifact_gib)):
        parser.error("resource limits must be positive")
    if len(set(args.datasets)) != len(args.datasets):
        parser.error("duplicate datasets")
    if "deep100m" in args.datasets and ("gist1m" not in args.datasets or
                                       args.datasets.index("gist1m") > args.datasets.index("deep100m")):
        parser.error("GIST import qualification must precede Deep trials")
    suite = Suite(args)
    def stop(signum, frame):
        raise RuntimeError("received stop signal %d" % signum)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        suite.execute()
    except BaseException as error:
        suite.status("stopped", error=str(error), evidence_preserved=True,
                     experiment_pools_may_remain=True)
        print("STOPPED: " + str(error), file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
