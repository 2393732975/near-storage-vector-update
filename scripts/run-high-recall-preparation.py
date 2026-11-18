#!/usr/bin/env python3
"""Unattended first batch: high-recall graph qualification and import speed trials.

This is preparation/qualification, not the 24-case formal baseline. It never
deploys CLS or enables frontier/executor optimizations. Failure leaves evidence.
"""

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import shutil
import signal
import statistics
import struct
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


def artifact_bytes(roots):
    """Count physical run artifacts once; never follow a results alias."""
    total, seen = 0, set()
    for root in roots:
        for path in root.rglob("*"):
            try:
                if path.is_symlink() or not path.is_file():
                    continue
                stat = path.stat()
                identity = (stat.st_dev, stat.st_ino)
                if identity not in seen:
                    total += stat.st_size
                    seen.add(identity)
            except FileNotFoundError:
                pass
    return total


def require_hash(path, expected):
    if (not isinstance(expected, str) or len(expected) != 64 or
            any(c not in "0123456789abcdef" for c in expected) or sha(path) != expected):
        raise RuntimeError("resume artifact hash mismatch: " + str(path))


def binary_header(path):
    with path.open("rb") as stream:
        data = stream.read(8)
    if len(data) != 8:
        raise RuntimeError("resume artifact has a truncated header: " + str(path))
    return struct.unpack("<II", data)


def validate_local_quality(document, split, width, minimum):
    count = split["count"]
    value = document.get("recall_at_k")
    rows = document.get("queries", [])
    if (document.get("evaluation") != "local_hnsw_screen_only" or
            document.get("query_ef") != width or document.get("recall_k") != 10 or
            document.get("evaluated_queries") != count or
            document.get("denominator") != count * 10 or len(rows) != count or
            not isinstance(value, (int, float)) or not math.isfinite(value) or
            not minimum <= value <= 1 or
            {x.get("query_index") for x in rows} != set(range(count))):
        raise RuntimeError("resume local quality evidence is incomplete or below its gate")
    if any(x.get("denominator") != 10 or not isinstance(x.get("hits"), int) or
           not 0 <= x["hits"] <= 10 for x in rows):
        raise RuntimeError("resume per-query quality evidence is invalid")
    hits = sum(x["hits"] for x in rows)
    if document.get("hits") != hits or not math.isclose(value, hits / (count * 10), abs_tol=1e-12):
        raise RuntimeError("resume local recall totals disagree")


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
        self.resume_root = Path(args.resume_from).resolve() if getattr(args, "resume_from", None) else None
        self.offline_only = bool(getattr(args, "offline_only", False))
        self.reused_splits = {}
        self.reused_graphs = {}
        self.reused_cases = {}
        self.reference_roots = []
        self.reference_bytes = 0
        self.resume_evidence = {}
        self.slow_guard = None
        self.cls_proof = None
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
        for name in ("baseline-quality.py", "index-content-digest.py", "baseline_cluster_guard.py",
                     "initialize-experiment-pools.sh", "cleanup-experiment-pools.sh"):
            shutil.copy2(self.repo / "scripts" / name, self.bin / name)
        if getattr(args, "retained_slow_evidence", None) and not self.offline_only:
            spec = importlib.util.spec_from_file_location("retained_slow_guard", self.bin / "baseline_cluster_guard.py")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            self.slow_guard = module.RetainedSlowAlertGuard(args.retained_slow_evidence, self.root, args.keyring)
        if getattr(args, "deployed_cls_evidence", None):
            proof = read_json(args.deployed_cls_evidence)
            digest = sha(self.bin / "libcls_hnsw_global.so")
            rows = proof.get("osds", [])
            if (proof.get("status") != "pass" or proof.get("fsid") != FSID or
                    len(rows) != 5 or {x.get("osd") for x in rows} != set(range(5)) or
                    proof.get("expected_sha256") != digest or
                    any(x.get("sha256") != digest or not x.get("matches_expected") for x in rows)):
                raise RuntimeError("deployed CLS proof is incomplete or binary differs")
            self.cls_proof = proof
            shutil.copy2(args.deployed_cls_evidence, self.root / "deployed-cls-verification.json")
        self.status("starting")

    def status(self, state, **extra):
        doc = dict(state=state, updated_utc=utc(), pid=os.getpid(),
                   elapsed_seconds=round(time.monotonic() - self.started, 1),
                   stage="offline_graph_qualification_only" if self.offline_only else
                         "preparation_and_qualification_only",
                   resume_from=str(self.resume_root) if self.resume_root else None,
                   referenced_artifact_bytes=self.reference_bytes,
                   steps=self.steps, **extra)
        write_json(self.root / "status.json", doc)

    def resource_gate(self):
        if time.monotonic() - self.started > self.args.max_hours * 3600:
            raise RuntimeError("first-batch wall-time budget reached")
        if shutil.disk_usage(str(self.root)).free < self.args.min_free_gib * 2**30:
            raise RuntimeError("local free-space reserve reached")
        size = artifact_bytes([self.root]) + self.reference_bytes
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
                        if not self.offline_only:
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
        detail = None
        guard = getattr(self, "slow_guard", None)
        if guard is not None:
            detail = guard.check()
        if initial:
            permitted = {"MGR_MODULE_ERROR"}
            if guard is not None:
                permitted.add("BLUESTORE_SLOW_OP_ALERT")
            if getattr(self, "resume_root", None) is not None and "POOL_NO_REDUNDANCY" in checks:
                if detail is None:
                    detail = self.ceph_json("health", "detail")
                warning = detail.get("checks", {}).get("POOL_NO_REDUNDANCY", {})
                expected_pools = {"nsvu_meta"} | {"nsvu_owner_%d" % i for i in range(5)}
                import re
                names = []
                for item in warning.get("detail", []):
                    found = re.findall(r"pool '([^']+)'", item.get("message", ""))
                    if len(found) != 1 or found[0] not in expected_pools:
                        raise RuntimeError("replica warning is outside the six authorized pools")
                    names += found
                if not names:
                    raise RuntimeError("replica warning lacks pool identities")
                permitted.add("POOL_NO_REDUNDANCY")
            if checks - permitted:
                raise RuntimeError("unexpected initial health checks: " + str(checks))
            self.allowed_health = checks | {"POOL_NO_REDUNDANCY", "LARGE_OMAP_OBJECTS"}
            if guard is not None:
                self.allowed_health.add("BLUESTORE_SLOW_OP_ALERT")
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

    def reuse_note(self, label, sources):
        record = dict(label=label, state="reused", verified_utc=utc(),
                      sources=[str(x) for x in sources])
        self.steps.append(record)
        self.status("running", current_step=label)

    def reference_path(self, value):
        path = Path(value).resolve()
        if not any(path == root or root in path.parents for root in self.reference_roots):
            raise RuntimeError("resume run artifact is outside its recorded lineage: " + str(path))
        return path

    def validate_prepared(self, dataset, path):
        document = read_json(path)
        if document.get("dataset") != dataset or document.get("schema_version") != 1:
            raise RuntimeError("resume split dataset/schema mismatch")
        cfg, base = document["config"], document["base"]
        if any(cfg[x] != base[x] for x in ("count", "dim", "kind")):
            raise RuntimeError("resume source vector configuration mismatch")
        for key in ("base", "source_queries", "source_ground_truth"):
            info = document[key]
            source = Path(info["path"])
            if source.stat().st_size != info["size_bytes"]:
                raise RuntimeError("resume source file size changed: " + str(source))
            if "mtime_ns" in info and source.stat().st_mtime_ns != info["mtime_ns"]:
                raise RuntimeError("resume source file metadata changed: " + str(source))
            require_hash(source, info["sha256"])
        declared, dim = binary_header(Path(base["path"]))
        row_bytes = cfg["dim"] * (4 if cfg["kind"] == "f32" else 1)
        if (declared < cfg["count"] or dim != cfg["dim"] or
                base["size_bytes"] != 8 + cfg["count"] * row_bytes):
            raise RuntimeError("resume base shape differs from actual data extent")
        query_source = document["source_queries"]
        source_gt = document["source_ground_truth"]
        if (cfg["kind"] not in ("f32", "u8") or cfg["metric"] not in ("l2", "ip") or
                binary_header(Path(query_source["path"])) != (query_source["declared_count"], cfg["dim"]) or
                binary_header(Path(source_gt["path"])) != (source_gt["count"], source_gt["width"]) or
                source_gt["count"] != query_source["count"] or source_gt["width"] < 10):
            raise RuntimeError("resume source query/GT schema mismatch")
        if set(document["splits"]) != {"calibration", "validation", "update"}:
            raise RuntimeError("resume split identities are incomplete")
        identities = set()
        with Path(query_source["path"]).open("rb") as vectors, \
                Path(source_gt["path"]).open("rb") as truths:
            for split in document["splits"].values():
                ids = split["original_query_ids"]
                if (len(ids) != split["count"] or len(set(ids)) != len(ids) or
                        any(type(i) is not int or not 0 <= i < query_source["count"] for i in ids) or
                        identities.intersection(ids)):
                    raise RuntimeError("resume query identities overlap or are invalid")
                identities.update(ids)
                vector_path = self.reference_path(split["vectors"])
                gt_path = self.reference_path(split["ground_truth"])
                require_hash(vector_path, split["vectors_sha256"])
                require_hash(gt_path, split["ground_truth_sha256"])
                if (binary_header(vector_path) != (split["count"], cfg["dim"]) or
                        vector_path.stat().st_size != 8 + split["count"] * row_bytes or
                        binary_header(gt_path) != (split["count"], source_gt["width"]) or
                        gt_path.stat().st_size != 8 + split["count"] * source_gt["width"] * 4):
                    raise RuntimeError("resume compact split shape changed")
                with vector_path.open("rb") as compact, gt_path.open("rb") as compact_gt:
                    compact.seek(8)
                    compact_gt.seek(8)
                    for identity in ids:
                        vectors.seek(8 + identity * row_bytes)
                        truths.seek(8 + identity * source_gt["width"] * 4)
                        if (compact.read(row_bytes) != vectors.read(row_bytes) or
                                compact_gt.read(source_gt["width"] * 4) != truths.read(source_gt["width"] * 4)):
                            raise RuntimeError("resume compact split no longer matches its original query IDs")
        verification_path = path.with_name("gt-verification.json")
        verification = read_json(verification_path)
        calibration = document["splits"]["calibration"]
        if (verification.get("status") != "pass" or verification.get("dataset") != dataset or
                verification.get("base_sha256") != base["sha256"] or
                verification.get("query_vectors_sha256") != calibration["vectors_sha256"] or
                verification.get("source_ground_truth_sha256") != calibration["ground_truth_sha256"] or
                verification.get("recall_k") != 10 or verification.get("sample_queries", 0) < 1 or
                len(verification.get("samples", [])) != verification["sample_queries"] or
                any(x.get("status") != "pass" for x in verification["samples"])):
            raise RuntimeError("resume exact GT sample verification is missing or mismatched")
        return document

    def validate_selected(self, dataset, manifest, path):
        selected = read_json(path)
        cache = self.reference_path(selected["cache"])
        require_hash(cache, selected["cache_sha256"])
        identity = dict(line.split("=", 1) for line in cache.with_name(cache.name + ".meta").read_text().splitlines())
        cfg = manifest["config"]
        wanted = dict(format="nsvu-hnsw-cache-v1", M=str(selected["M"]),
                      ef_construction=str(selected["ef_construction"]), num_vectors=str(cfg["count"]),
                      dim=str(cfg["dim"]), vector_kind="2" if cfg["kind"] == "f32" else "1",
                      metric="2" if cfg["metric"] == "ip" else "1",
                      dataset_sha256=manifest["base"]["sha256"], threads=str(cfg["threads"]), seed="100")
        if (any(identity.get(k) != v for k, v in wanted.items()) or
                selected["ef_update"] != selected["ef_query"] or
                selected.get("local_quality_only") is not True or
                selected.get("remote_and_dynamic_quality") != "not_yet_certified"):
            raise RuntimeError("resume cache metadata or quality-role mismatch")
        width = selected["ef_query"]
        validate_local_quality(read_json(cache.parent / ("calibration-ef%d.json" % width)),
                               manifest["splits"]["calibration"], width, .92)
        validate_local_quality(read_json(cache.parent / "validation.json"),
                               manifest["splits"]["validation"], width, .90)
        return selected

    def reusable_case(self, row, dataset, manifest, selected):
        folder = self.reference_path(row["path"])
        required = [folder / "import.json", folder / "checker.json"]
        if dataset == "gist1m":
            required.append(folder / "content-digest.json")
        if not all(path.is_file() for path in required):
            return None  # A partial import is never certified or resumed in place.
        metrics, checker = read_json(required[0]), read_json(required[1])
        cfg = manifest["config"]
        expected = dict(M=selected["M"], ef_construction=selected["ef_construction"],
                        num_vectors=cfg["count"], vectors_imported=cfg["count"], dim=cfg["dim"],
                        points_per_object=cfg["points_per_object"], owners=5,
                        import_workers=row["workers"], omap_batch=row["omap_batch"])
        if (row.get("metrics") != metrics or any(metrics.get(k) != v for k, v in expected.items()) or
                not isinstance(metrics.get("total_seconds"), (int, float)) or
                not math.isfinite(metrics["total_seconds"]) or metrics["total_seconds"] <= 0):
            raise RuntimeError("resume import metrics are inconsistent with the frozen graph")
        if (checker.get("status") != "pass" or checker.get("errors") or checker.get("warnings") or
                checker.get("nodes_checked") != cfg["count"] or
                checker.get("nodes_expected") != cfg["count"] or
                checker.get("label_mappings") != cfg["count"]):
            return None
        if dataset == "gist1m":
            digest = read_json(required[2])
            if digest.get("fsid") != FSID or not digest.get("objects"):
                raise RuntimeError("resume content digest is missing its object/cluster identity")
            value = digest.get("logical_sha256")
            calculated = hashlib.sha256(json.dumps(digest["objects"], sort_keys=True,
                                                   separators=(",", ":")).encode()).hexdigest()
            if value != calculated:
                raise RuntimeError("resume logical content digest does not match its records")
        return dict(row, reused_from=str(folder))

    def load_resume(self):
        if self.resume_root is None:
            return
        if self.resume_root == self.root:
            raise RuntimeError("resume must use a distinct new RUN_ROOT")
        prior = read_json(self.resume_root / "manifest.json")
        state = read_json(self.resume_root / "status.json")
        if state.get("state") in ("starting", "running"):
            raise RuntimeError("resume source is still marked active")
        ancestry = self.resume_root / "resume-manifest.json"
        ancestor_roots = read_json(ancestry).get("ancestor_roots", []) if ancestry.exists() else []
        self.reference_roots = list(dict.fromkeys([self.resume_root] + [Path(x).resolve() for x in ancestor_roots]))
        if self.root in self.reference_roots:
            raise RuntimeError("resume ancestry contains new RUN_ROOT")
        for root in self.reference_roots:
            if read_json(root / "manifest.json").get("expected_fsid") != FSID:
                raise RuntimeError("resume lineage cluster identity mismatch")
        for name in ("nsvu-base-importer", "nsvu-index-checker", "libcls_hnsw_global.so",
                     "baseline-quality.py", "index-content-digest.py"):
            if prior.get("binaries", {}).get(name) != sha(self.bin / name):
                raise RuntimeError("resume computational binary/tool changed: " + name)
        files = {}
        for dataset in DATASETS:
            source_split = self.resume_root / "splits" / dataset / "split-manifest.json"
            source_selected = self.resume_root / "graphs" / dataset / "selected.json"
            if not source_split.exists():
                if source_selected.exists():
                    raise RuntimeError("resume selected graph has no split identity")
                continue
            self.status("running", current_step="verify-resume-" + dataset)
            manifest = self.validate_prepared(dataset, source_split)
            destination = self.root / "splits" / dataset
            destination.mkdir(parents=True)
            for source in (source_split, source_split.with_name("gt-verification.json")):
                shutil.copy2(source, destination / source.name)
                files[str(source)] = sha(source)
            self.reused_splits[dataset] = manifest
            if source_selected.exists():
                selected = self.validate_selected(dataset, manifest, source_selected)
                target = self.root / "graphs" / dataset / "selected.json"
                target.parent.mkdir(parents=True)
                shutil.copy2(source_selected, target)
                files[str(source_selected)] = sha(source_selected)
                self.reused_graphs[dataset] = selected
            source_summary = self.resume_root / "import-trials" / dataset / "summary.json"
            if source_summary.exists() and dataset in self.reused_graphs:
                seen, digests, rows = set(), set(), []
                for row in read_json(source_summary):
                    key = (row["repetition"], row["workers"], row["omap_batch"])
                    if key in seen:
                        raise RuntimeError("duplicate resume import case identity")
                    seen.add(key)
                    verified = self.reusable_case(row, dataset, manifest, self.reused_graphs[dataset])
                    if verified is not None:
                        rows.append(verified)
                        if dataset == "gist1m":
                            digests.add(read_json(Path(row["path"]) / "content-digest.json")["logical_sha256"])
                if len(digests) > 1:
                    raise RuntimeError("resume GIST import contents disagree")
                self.reused_cases[dataset] = {(x["repetition"], x["workers"], x["omap_batch"]): x for x in rows}
                expected = {(rep, w, b) for rep in (1, 2, 3) for w, b in ((1, 2048), (2, 2048), (5, 2048), (5, 8192))}
                if dataset == "gist1m" and set(self.reused_cases[dataset]) == expected:
                    target = self.root / "import-trials" / dataset
                    write_json(target / "summary.json", rows)
                    settings = self.resume_root / "import-trials" / dataset / "selected-settings.json"
                    settings_doc = read_json(settings)
                    options = {(x["workers"], x["omap_batch"]) for x in rows}
                    medians = {item: statistics.median(x["metrics"]["total_seconds"] for x in rows
                               if (x["workers"], x["omap_batch"]) == item) for item in options}
                    choice = (settings_doc["workers"], settings_doc["omap_batch"])
                    if choice not in medians or medians[choice] > min(medians.values()) + 1e-9:
                        raise RuntimeError("resume selected import settings are not supported by complete trials")
                    shutil.copy2(settings, target / settings.name)
                    files[str(settings)] = sha(settings)
                files[str(source_summary)] = sha(source_summary)
        self.reference_bytes = artifact_bytes(self.reference_roots)
        self.resume_evidence = dict(source_run=str(self.resume_root), verified_utc=utc(),
                                    ancestor_roots=[str(x) for x in self.reference_roots],
                                    referenced_artifact_bytes=self.reference_bytes, source_files_sha256=files,
                                    local_quality_only=True, failed_imports_reused=False)
        write_json(self.root / "resume-manifest.json", self.resume_evidence)
        self.resource_gate()
        self.reuse_note("resume-evidence-verified", [self.resume_root])

    def prepare(self, dataset):
        if dataset in self.reused_splits:
            path = self.root / "splits" / dataset / "split-manifest.json"
            self.reuse_note(dataset + "-reuse-split", [path])
            return path, self.reused_splits[dataset]
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
        if dataset in self.reused_graphs:
            self.reuse_note(dataset + "-reuse-qualified-graph", [self.root / "graphs" / dataset / "selected.json"])
            return self.reused_graphs[dataset]
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
            identities = {(r, w, b) for r in (1, 2, 3) for w, b in configurations}
            if set(self.reused_cases.get(dataset, {})) == identities:
                self.reuse_note(dataset + "-reuse-complete-import-trials",
                                [self.root / "import-trials" / dataset / "summary.json"])
                return
        elif dataset == "deep100m":
            prior = read_json(self.root / "import-trials/gist1m/selected-settings.json")
            configurations = list(dict.fromkeys([(1, 2048), (prior["workers"], prior["omap_batch"])]))
            repetitions = 1
        else:
            return
        for repetition in range(1, repetitions + 1):
            order = configurations if repetition % 2 else list(reversed(configurations))
            for workers, batch in order:
                key = (repetition, workers, batch)
                prior = self.reused_cases.get(dataset, {}).get(key)
                if prior is not None:
                    cases.append(prior)
                    if dataset == "gist1m":
                        value = read_json(Path(prior["path"]) / "content-digest.json")["logical_sha256"]
                        if expected is not None and value != expected:
                            raise RuntimeError("parallel import changed persisted logical content")
                        expected = value
                    self.reuse_note(dataset + "-reuse-import-r%d-w%d-b%d" % key, [prior["path"]])
                    write_json(self.root / "import-trials" / dataset / "summary.json", cases)
                    continue
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
        if not self.offline_only:
            self.cluster_gate(initial=True)
        versions = {p.name: sha(p) for p in self.bin.iterdir()}
        write_json(self.root / "manifest.json", dict(
            started_utc=utc(), source_commit=subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=str(self.repo), text=True).strip(),
            expected_fsid=FSID, datasets=list(self.args.datasets), binaries=versions,
            scope="offline quality qualification and import speed trials, not formal baseline",
            frontier_window=1, executor_enabled=False, max_hours=self.args.max_hours,
            offline_only=self.offline_only,
            cluster_validation="not_requested_offline_only" if self.offline_only else "passed_startup_gate",
            resume_from=str(self.resume_root) if self.resume_root else None,
            retained_slow_warning_guard=bool(getattr(self, "slow_guard", None)),
            deployed_cls_verification="all five deployed files verified" if getattr(self, "cls_proof", None)
                                      else "remote deployed files not certified by this run",
            deployed_cls_evidence=self.cls_proof if getattr(self, "cls_proof", None) else None))
        self.load_resume()
        if (not self.offline_only and "deep100m" in self.args.datasets and "gist1m" not in self.args.datasets and
                not (self.root / "import-trials/gist1m/selected-settings.json").exists()):
            raise RuntimeError("Deep-only resume requires complete verified GIST import trials")
        for dataset in self.args.datasets:
            _, manifest = self.prepare(dataset)
            selected = self.qualify(dataset, manifest)
            if not self.offline_only:
                self.import_trials(dataset, manifest, selected)
        if self.offline_only:
            self.status("offline_qualification_complete", experiment_pools_untouched=True,
                        next_step="Resolve/qualify cluster health, then resume this run for remaining import trials; "
                                  "remote and dynamic baseline quality remains uncertified.")
            return
        # Pool cleanup is restricted to the same six experiment pools. Code 3
        # means cleanup succeeded and the pre-existing health warning remains.
        self.cluster_gate()
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
    parser.add_argument("--resume-from", type=Path,
                        help="Verify/reuse completed evidence from a prior run into a fresh RUN_ROOT")
    parser.add_argument("--offline-only", action="store_true",
                        help="Only split/graph quality work; never query/change/clean Ceph")
    parser.add_argument("--retained-slow-evidence", type=Path)
    parser.add_argument("--deployed-cls-evidence", type=Path)
    parser.add_argument("--dataset-root", type=Path, default=Path("/home/hhf/datasets"))
    parser.add_argument("--keyring", type=Path, default=Path("/home/hhf/study/ceph.client.admin.keyring"))
    parser.add_argument("--python", default="/tmp/nsvu-osd-plot-venv/bin/python")
    parser.add_argument("--datasets", nargs="+", choices=DATASETS, default=list(DATASETS))
    parser.add_argument("--max-hours", type=float, default=36)
    parser.add_argument("--min-free-gib", type=float, default=250)
    parser.add_argument("--max-artifact-gib", type=float, default=700)
    args = parser.parse_args()
    if not args.confirm_reset and not args.offline_only:
        parser.error("--confirm-reset is required for authorized experiment-pool resets")
    if any(x <= 0 for x in (args.max_hours, args.min_free_gib, args.max_artifact_gib)):
        parser.error("resource limits must be positive")
    if len(set(args.datasets)) != len(args.datasets):
        parser.error("duplicate datasets")
    if not args.resume_from and not args.offline_only and "deep100m" in args.datasets and ("gist1m" not in args.datasets or
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
