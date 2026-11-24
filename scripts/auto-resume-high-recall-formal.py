#!/usr/bin/env python3
"""Resume the frozen four-OSD formal experiment after a BlueStore slow event."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import re
import shlex
import shutil
import signal
import subprocess
import sys
from tempfile import TemporaryDirectory, TemporaryFile
import time


REPO = Path(__file__).resolve().parents[1]
RESULTS = REPO / "results"
LATEST = RESULTS / "high-recall-formal.latest-launch.json"
STOP_REASON = "New OSD slow, restart or device events during formal work"
SLOW_EVENT = re.compile(
    r"^[^\n]*?bluestore\(/var/lib/ceph/osd/ceph-(?P<osd>\d+)\) "
    r"(?:_txc_state_proc slow aio_wait, txc = 0x[0-9a-f]+, latency = [0-9.]+s|"
    r"log_latency_fn slow operation observed for _txc_committed_kv, "
    r"latency = [0-9.]+s, txc = 0x[0-9a-f]+|"
    r"log_latency slow operation observed for kv_(?:commit|sync), latency = [0-9.]+s)\s*$"
)


class RetryableRecovery(RuntimeError):
    """The stopped case can be run again after a verified quiet period."""


def read(path):
    return json.loads(path.read_text())


def slow_events(events, active_osds):
    if not isinstance(events, list) or not events:
        return False
    for row in events:
        if (not isinstance(row, dict) or type(row.get("osd")) is not int or
                row["osd"] not in active_osds or type(row.get("timestamp_us")) is not int or
                row["timestamp_us"] <= 0 or not isinstance(row.get("message"), str)):
            return False
        match = SLOW_EVENT.fullmatch(row["message"])
        if not match or int(match["osd"]) != row["osd"]:
            return False
    return True


def stopped_events(path, active_osds):
    status = read(path / "status.json")
    if status.get("state") != "stopped" or status.get("error") != STOP_REASON:
        raise RuntimeError("Previous run did not stop at the formal OSD log guard")
    events = read(path / "evidence/ceph-guard-unexpected-events.json")
    if not slow_events(events, active_osds):
        raise RuntimeError("The stop is not a recognized BlueStore slow operation on an active OSD")
    return events


def load_formal_runner(template):
    sys.path.insert(0, str(template))
    spec = importlib.util.spec_from_file_location("frozen_formal_runner", template / "run.py")
    formal = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(formal)
    import ceph_guard
    return formal, ceph_guard


def scan_journal(rows, osd, prior, is_event, *, tail=False):
    anchor, events, count = None, [], 0
    for line in rows:
        row = json.loads(line)
        cursor, boot, invocation = (row.get(key) for key in (
            "__CURSOR", "_BOOT_ID", "_SYSTEMD_INVOCATION_ID"))
        if not cursor or not boot or not invocation or not isinstance(row.get("MESSAGE"), str):
            raise RuntimeError(f"osd.{osd} journal identity or record changed")
        if count == 0:
            if prior and not tail and cursor != prior["cursor"]:
                raise RuntimeError(f"osd.{osd} journal anchor missing")
            origin = prior or {"boot": boot, "invocation": invocation}
        if (boot, invocation) != (origin["boot"], origin["invocation"]):
            raise RuntimeError(f"osd.{osd} journal daemon identity changed")
        if (count or prior is None or tail) and is_event(row):
            events.append(dict(osd=osd, timestamp_us=int(row["__REALTIME_TIMESTAMP"]), message=row["MESSAGE"]))
        anchor = dict(cursor=cursor, boot=boot, invocation=invocation)
        count += 1
    if anchor is None:
        raise RuntimeError(f"osd.{osd} journal coverage could not be verified")
    return osd, anchor, events, count - int(bool(prior) and not tail)


def recovery_guard(base, active_osds, journal_module):
    class Guard(base):
        def _read_journals(self, *, first_since=None, tails=False):
            def read_osd(osd):
                prior = self.anchors.get(osd)
                if tails:
                    selectors = ["-n", "1"]
                elif prior is not None:
                    selectors = ["--cursor", prior["cursor"]]
                else:
                    selectors = ["--since", first_since]
                args = ["journalctl", "-u", f"ceph-{journal_module.FSID}@osd.{osd}.service",
                        "--no-pager", "--quiet", "--all", "-o", "json",
                        "--output-fields=MESSAGE,_BOOT_ID,_SYSTEMD_INVOCATION_ID", *selectors]
                host = journal_module.OSD_HOSTS[osd]
                command = ["sudo", "-n", *args]
                if host:
                    command = [
                        "ssh", "-F", "/dev/null", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
                        "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes",
                        "-o", "GlobalKnownHostsFile=/dev/null",
                        "-o", f"UserKnownHostsFile={journal_module.SSH_KNOWN_HOSTS}",
                        "-i", str(self.identity), "root@" + host, shlex.join(args),
                    ]
                # Spool the projection to disk so a long stop does not need an
                # in-memory copy of every journal row before checking its cursor.
                with TemporaryFile() as rows:
                    try:
                        result = subprocess.run(command, stdout=rows, stderr=subprocess.PIPE, timeout=60)
                    except (OSError, subprocess.TimeoutExpired) as error:
                        raise RuntimeError(f"osd.{osd} journal could not be read") from error
                    if result.returncode or result.stderr.strip():
                        raise RuntimeError(f"osd.{osd} journal could not be read, exit={result.returncode}")
                    rows.seek(0)
                    return scan_journal(rows, osd, prior, journal_module.is_event, tail=tails)

            with ThreadPoolExecutor(max_workers=len(active_osds)) as workers:
                samples = list(workers.map(read_osd, sorted(active_osds)))
            events = [event for _, _, records, _ in samples for event in records]
            return ({osd: anchor for osd, anchor, _, _ in samples},
                    sorted(events, key=lambda event: event["timestamp_us"]),
                    sum(count for _, _, _, count in samples))

        def anchor_clean(self):
            # A cleared alert does not replace the previous journal checkpoint.
            self._recover(set())

        def _recover(self, health):
            previous = read(self.recovery_source / "ceph-guard-status.json")
            expected = read(self.recovery_source / "ceph-guard-unexpected-events.json")
            self.anchors = {int(osd): anchor for osd, anchor in previous["journal_anchors"].items()}
            if set(self.anchors) != set(active_osds) or not slow_events(expected, active_osds):
                raise RuntimeError("Prior checkpoint lacks complete OSD journal evidence")
            admitted = set(previous["retained_slow_osds"]) | {row["osd"] for row in expected}
            if health - admitted:
                raise RuntimeError("BlueStore warnings expanded beyond verified OSDs")

            anchors, events, checked = self._read_journals()
            event_key = lambda row: (row["osd"], row["timestamp_us"], row["message"])
            if Counter(map(event_key, events)) != Counter(map(event_key, expected)):
                raise RuntimeError("OSD journals contain events beyond the recorded stop")
            self.anchors = anchors
            self.quiet_samples = self._quiet_evidence() + self._quiet_evidence()
            if any(sample != self.quiet_samples[0] for sample in self.quiet_samples):
                raise RetryableRecovery("OSD counters changed across the quiet windows")
            anchors, extra, extra_count = self._read_journals()
            if extra:
                raise RuntimeError("New OSD journal events during the quiet windows")
            observed = self._alert_ids()
            if observed - admitted:
                raise RuntimeError("BlueStore warnings expanded during recovery")
            self.anchors = anchors
            self.checked = checked + extra_count
            self.admissions = 1
            self.reset_warning_osds = observed
            self._record("recovering_from_verified_previous_stop", expected)

        def _quiet_evidence(self):
            # Check read_eio independently so it cannot be treated as a busy OSD.
            first = self._sample()
            time.sleep(10)
            last = self._sample()
            if any(row["read_eio"] for sample in (first, last) for row in sample.values()):
                raise RuntimeError("OSD perf counters report read I/O errors")
            for osd, row in first.items():
                if (last[osd] != row or any(row[key] for key in ("num_ops", "op_wip", "finisher_queue"))):
                    raise RetryableRecovery(f"osd.{osd} is still busy")
            return [first, last]

    return Guard


def wait_for_quiet(operation, suite, phase, wait_seconds):
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            return operation()
        except RuntimeError as error:
            if not isinstance(error.__cause__, RetryableRecovery) or time.monotonic() >= deadline:
                raise
            suite.resource_gate()
            phase("waiting_for_quiet_osd_io", reason=str(error))
            time.sleep(min(15, max(0, deadline - time.monotonic())))


def suite_for(formal, root, source_manifest):
    evidence = root / "evidence"
    evidence.mkdir()
    formal.qualify_devices()
    layout = source_manifest["storage_layout"]
    suite = formal.runner.Suite(argparse.Namespace(
        run_root=root / "pilot", keyring=formal.KEYRING, dataset_root=Path("/home/hhf/datasets"),
        python=sys.executable, datasets=formal.DATASETS, max_hours=7 * 24,
        min_free_gib=250, max_artifact_gib=700, confirm_reset=True,
        resume_from=formal.prior.PRIOR, offline_only=False, owner_osds=layout["owner_osds"],
        excluded_osds=layout["excluded_osds"], resume_local_only=True,
        cohort_id="formal-uniform-deep100m-parameters", retained_slow_evidence=None,
        deployed_cls_evidence=formal.prior.QUAL / "deployed-cls-verification.json",
        device_qualification=evidence / "device-qualification.json"))
    suite.allowed_health = formal.HEALTH | {formal.ALERT}
    shutil.copy2(formal.RR_IMPORT / "bin/nsvu-base-importer", suite.bin / "nsvu-round-robin-importer")
    actual = {p.name: formal.sha(p) for p in suite.bin.iterdir() if p.is_file()}
    if actual != source_manifest["binaries"]:
        raise RuntimeError("Frozen benchmark binary or script hashes differ")
    shutil.copy2(formal.RR_IMPORT / "importer.patch", suite.root / "importer.patch")
    return suite


def verify_previous_pools(suite, placement):
    suite.verify_pool_placement()
    actual = read(suite.root / "pool-placement-latest.json")["pools"]
    if actual != placement["pools"]:
        raise RuntimeError("Live pool IDs or placement differ from the stopped case")
    names = {pool["pool_name"] for pool in suite.ceph_json("osd", "pool", "ls", "detail")
             if pool["pool_name"].startswith("nsvu_")}
    if names != {pool["pool"] for pool in actual}:
        raise RuntimeError("Live experiment pool names differ from the stopped case")


def verify_device_layout(suite):
    metadata = {row["id"]: row for row in suite.ceph_json("osd", "metadata")}
    for device in suite.device_proof["devices"]:
        actual = metadata.get(device["osd"], {})
        if (actual.get("hostname") != device["host"] or
                device["serial"] not in actual.get("device_ids", "") or
                Path(device["device"]).name not in actual.get("devices", "").split(",")):
            raise RuntimeError(f"osd.{device['osd']} live device identity changed")


def checkpoints(formal, previous):
    completed = {}
    interrupted = []
    for dataset in formal.DATASETS:
        for rep, modes in formal.REPEAT_ORDER.items():
            for mode in modes:
                key = dataset, rep, mode
                directory = previous / "pilot/cases" / dataset / f"rep-{rep}" / mode
                if not directory.exists():
                    continue
                case = read(directory / "case.json")
                if (case["dataset"], case["rep"], case["route"]) != key:
                    raise RuntimeError(f"Case identity differs from its directory: {directory}")
                if case["status"] == "completed":
                    count = formal.UPDATES[dataset]
                    formal.validate_route(directory / "update.json", mode, count)
                    formal.clean_trace(directory / "updates.jsonl", count)
                    # Preserve completed quality measurements, including recorded failures.
                    for name in ("final-score.json", "checker.json"):
                        if "status" not in read(directory / name):
                            raise RuntimeError(f"Completed case lacks {name} status: {directory}")
                    completed[key] = directory
                elif case["status"] == "stopped" and case.get("error") == STOP_REASON:
                    interrupted.append(key)
                    placement = case["pool_placement"]
                else:
                    raise RuntimeError(f"Case needs review before resuming: {directory}")
    pending = [(d, rep, mode) for d in formal.DATASETS for rep, modes in formal.REPEAT_ORDER.items()
               for mode in modes if (d, rep, mode) not in completed]
    if len(interrupted) != 1 or not pending or interrupted[0] != pending[0]:
        raise RuntimeError("Stopped case is not the next unfinished formal case")
    return completed, pending, placement


def publish_attempt(formal, root, template, previous, source_manifest, completed, launcher_log):
    for directory in completed.values():
        shutil.copytree(directory, root / "pilot/cases" / directory.relative_to(previous / "pilot/cases"))
    manifest = dict(source_manifest, previous_formal_run=str(previous),
                    reused_completed_cases=sorted(completed),
                    orchestration_commit=subprocess.check_output(
                        ["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip())
    formal.write(root / "manifest.json", manifest)
    formal.update_summary()
    latest = dict(run_root=str(root / "pilot"), status=str(root / "status.json"),
                  summary=str(root / "formal-summary.json"),
                  graph_build_status=str(root / "graph-build-status.json"),
                  previous_root=str(previous), runner_template=str(template))
    if launcher_log:
        latest["launcher_log"] = str(launcher_log)
    formal.write(LATEST, latest)


def run_cases(formal, suite, guard, pending, placement, reset_wait):
    def gate():
        base_gate()
        formal.validate_health(suite.ceph_json("health", "detail"), permit_retained=True)
        guard.check()

    base_gate = suite.cluster_gate
    suite.cluster_gate = gate
    for dataset, rep, mode in pending:
        label = f"{dataset}-rep{rep}-{mode}"
        manifest = formal.prior.PRIOR / "splits" / dataset / "split-manifest.json"
        if read(manifest)["dataset"] != dataset:
            raise RuntimeError(f"Frozen split belongs to a different dataset: {dataset}")
        graph = formal.graph_for(suite, dataset, rep)
        formal.runner.require_hash(Path(graph["cache"]), graph["cache_sha256"])
        formal.phase("recreating_experiment_pools", dataset=dataset, rep=rep, route=mode)
        suite.resource_gate()
        verify_previous_pools(suite, placement)
        gate()
        started = formal.runner.utc()
        suite.run(label + "-reset", ["bash", suite.bin / "initialize-experiment-pools.sh",
                                    "--confirm-reset"], 600)
        ended = formal.runner.utc()
        wait_for_quiet(lambda: guard.admit_reset(started, ended), suite, formal.phase, reset_wait)
        formal.wait_for_clean_placement(suite, base_gate)
        placement = read(suite.root / "pool-placement-latest.json")
        formal.verify_empty_pools(suite, {p["pool"]: p["pool_id"] for p in placement["pools"]})
        gate()
        try:
            formal.execute_case(suite, dataset, manifest, graph, rep, mode, placement)
        except RuntimeError as error:
            if str(error) == STOP_REASON:
                raise RetryableRecovery(str(error)) from error
            raise


def attempt(formal, guard_module, template, previous, root, args):
    formal.ROOT = root
    source_manifest = read(formal.SOURCE / "manifest.json")
    previous_manifest = read(previous / "manifest.json")
    for field in ("source_commit", "binaries", "storage_layout", "HNSW", "update_counts"):
        if previous_manifest[field] != source_manifest[field]:
            raise RuntimeError(f"Stopped run has a different frozen {field}")
    preparation = "scripts/run-high-recall-preparation.py"
    archived = subprocess.check_output(
        ["git", "show", f"{source_manifest['source_commit']}:{preparation}"], cwd=REPO)
    if (REPO / preparation).read_bytes() != archived:
        raise RuntimeError("Preparation runner differs from the frozen source")
    active = source_manifest["storage_layout"]["owner_osds"]
    if set(active) != set(guard_module.OSD_HOSTS) or source_manifest["expected_fsid"] != formal.runner.FSID:
        raise RuntimeError("Frozen cohort does not match the formal OSD guard")
    stopped_events(previous, active)
    completed, pending, placement = checkpoints(formal, previous)
    if read(template / "graphs.json") != read(previous / "graphs.json"):
        raise RuntimeError("Frozen graph catalogue differs from the stopped run")
    for name in ("graphs.json", "graph-build-status.json"):
        shutil.copy2(template / name, root / name)
    suite = suite_for(formal, root, source_manifest)
    try:
        suite.cluster_gate()
        if suite.ceph_json("versions") != source_manifest["ceph_versions"]:
            raise RuntimeError("Live Ceph daemon versions differ from the frozen run")
        formal.validate_health(suite.ceph_json("health", "detail"), permit_retained=True)
        verify_device_layout(suite)
        verify_previous_pools(suite, placement)
        with ExitStack() as stack:
            guard_type = recovery_guard(guard_module.Guard, active, guard_module)
            guard = wait_for_quiet(lambda: stack.enter_context(
                guard_type(root / "evidence", recovery_source=previous / "evidence")),
                suite, formal.phase, args.wait_seconds)
            suite.cluster_gate()
            guard.check()
            verify_previous_pools(suite, placement)
            if not args.confirm_reset:
                print(json.dumps(dict(state="ready", next_case=pending[0],
                                      completed_cases=len(completed), pending_cases=len(pending))), flush=True)
                return
            publish_attempt(formal, root, template, previous, source_manifest, completed, args.launcher_log)
            run_cases(formal, suite, guard, pending, placement, args.wait_seconds)
            suite.status("completed")
            formal.phase("completed", summary=str(root / "formal-summary.json"))
    except Exception as error:
        suite.status("stopped", error=str(error))
        formal.phase("stopped", error=str(error))
        raise
    finally:
        suite.lock.close()


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("expected a positive integer")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-run", type=Path, help="Stopped run directory, defaults to the latest formal run")
    parser.add_argument("--template", type=Path, help="Directory holding the frozen formal runner and graph list")
    parser.add_argument("--confirm-reset", action="store_true",
                        help="Recreate the five experiment pools before each pending case")
    parser.add_argument("--max-attempts", type=positive_int, default=3, help="Maximum resumed runs per launch")
    parser.add_argument("--wait-seconds", type=positive_int, default=600, help="Time to wait when OSD I/O is busy")
    parser.add_argument("--launcher-log", type=Path, help="Path used for the redirected supervisor output")
    args = parser.parse_args()
    latest = read(LATEST)
    previous = (args.from_run or Path(latest["status"]).parent).resolve()
    template = (args.template or Path(latest.get("runner_template", previous))).resolve()
    formal, guard_module = load_formal_runner(template)
    if args.launcher_log:
        args.launcher_log = args.launcher_log.resolve()
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, formal.stop)
    if not args.confirm_reset:
        with TemporaryDirectory(prefix="nsvu-formal-recovery-check-", dir=RESULTS) as folder:
            attempt(formal, guard_module, template, previous, Path(folder), args)
        return 0
    for number in range(1, args.max_attempts + 1):
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        root = RESULTS / f"high-recall-formal-auto-resume-{stamp}"
        root.mkdir()
        print(f"Attempt {number}/{args.max_attempts}: {root}", flush=True)
        try:
            attempt(formal, guard_module, template, previous, root, args)
            return 0
        except RetryableRecovery:
            stopped_events(root, guard_module.OSD_HOSTS)
            previous = root
    raise RuntimeError("Automatic recovery attempt limit reached")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RuntimeError, OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        sys.exit(str(error))
