#!/usr/bin/env python3
"""Resume independent offline work, then retry incomplete import qualification."""

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time


def utc():
    return datetime.now(timezone.utc).isoformat()


def read(path):
    return json.loads(path.read_text())


def write(path, document):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(document, indent=2) + "\n")
    temporary.replace(path)


def validate_cls(document, binary):
    expected = hashlib.sha256(binary.read_bytes()).hexdigest()
    rows = document.get("osds", [])
    if (document.get("status") != "pass" or
            document.get("fsid") != "c83da116-1e8b-11f1-933d-cd48aa44d216" or
            {x.get("osd") for x in rows} != set(range(5)) or len(rows) != 5 or
            document.get("expected_sha256") != expected or
            any(x.get("sha256") != expected or not x.get("matches_expected") for x in rows)):
        raise RuntimeError("deployed CLS evidence does not match all five OSDs and local binary")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--confirm-reset", action="store_true")
    parser.add_argument("--source-run", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--retained-slow-evidence", type=Path, required=True)
    parser.add_argument("--deployed-cls-evidence", type=Path, required=True)
    parser.add_argument("--max-hours", type=float, default=36)
    args = parser.parse_args()
    if not args.confirm_reset:
        parser.error("--confirm-reset is required for the authorized import retry")
    if args.max_hours <= 0:
        parser.error("--max-hours must be positive")
    repo = Path(__file__).resolve().parents[1]
    root = args.run_root.resolve()
    if root.exists() and any(root.iterdir()):
        parser.error("run-root must be new or empty")
    if read(args.source_run / "status.json").get("state") != "stopped":
        parser.error("source must be a stopped run; active/successful runs cannot be relaunched here")
    validate_cls(read(args.deployed_cls_evidence), repo / "build/libcls_hnsw_global.so")
    root.mkdir(parents=True, exist_ok=True)
    lock = (repo / "results/.high-recall-continuation.lock").open("w")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    for source, name in ((args.retained_slow_evidence, "retained-slow-evidence.json"),
                         (args.deployed_cls_evidence, "deployed-cls-verification.json")):
        shutil.copy2(source, root / name)
    # Keep the controller and child runner/helper immutable during the detached
    # continuation. The child module derives repo from its original file path,
    # so verify source hashes before each phase instead of changing that path.
    source_names = ("run-high-recall-preparation.py", "baseline_cluster_guard.py")
    identities = {name: hashlib.sha256((repo / "scripts" / name).read_bytes()).hexdigest()
                  for name in source_names}
    write(root / "manifest.json", dict(started_utc=utc(), source_run=str(args.source_run.resolve()),
          source_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(repo), text=True).strip(),
          scope="continue preparation only; no formal baseline cases", scripts=identities,
          max_hours=args.max_hours, cls_verified_osds=5,
          retained_warning_policy="preserve warning; stop on any new journal event or affected OSD"))
    child = None
    started = time.monotonic()
    phases = []
    def stop(signum, frame):
        raise RuntimeError("continuation received stop signal %d" % signum)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        previous = args.source_run.resolve()
        for name, extra, terminal in (
                ("offline-sift", ["--datasets", "sift100m", "--offline-only"], "offline_qualification_complete"),
                ("remaining-imports", ["--retained-slow-evidence", str(root / "retained-slow-evidence.json"),
                                       "--deployed-cls-evidence", str(root / "deployed-cls-verification.json")],
                 "first_batch_complete")):
            for filename, expected in identities.items():
                if hashlib.sha256((repo / "scripts" / filename).read_bytes()).hexdigest() != expected:
                    raise RuntimeError("continuation source changed before next phase: " + filename)
            remaining = args.max_hours - (time.monotonic() - started) / 3600
            if remaining <= 0:
                raise RuntimeError("continuation wall-time budget reached")
            output = root / name
            cmd = [sys.executable, str(repo / "scripts/run-high-recall-preparation.py"), "--confirm-reset",
                   "--run-root", str(output), "--resume-from", str(previous),
                   "--max-hours", str(remaining), *extra]
            phase = dict(name=name, run_root=str(output), started_utc=utc(), state="running")
            phases.append(phase)
            print("[%s] START %s" % (utc(), name), flush=True)
            with (root / (name + ".launcher.log")).open("w") as log:
                child = subprocess.Popen(cmd, cwd=str(repo), stdout=log, stderr=subprocess.STDOUT,
                                         stdin=subprocess.DEVNULL, start_new_session=True)
                phase["pid"] = child.pid
                while child.poll() is None:
                    if time.monotonic() - started > args.max_hours * 3600:
                        raise RuntimeError("continuation wall-time budget reached")
                    progress = read(output / "status.json") if (output / "status.json").exists() else {}
                    write(root / "status.json", dict(state="running", pid=os.getpid(), updated_utc=utc(),
                          current_phase=name, child_status=progress, phases=phases,
                          scope="preparation and qualification; formal baseline not yet started"))
                    try:
                        child.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        pass
            result = child.returncode
            child = None
            progress = read(output / "status.json") if (output / "status.json").exists() else {}
            if result or progress.get("state") != terminal:
                raise RuntimeError("phase %s stopped: %s" % (name, progress.get("error", result)))
            phase.update(state="completed", finished_utc=utc())
            previous = output
        write(root / "status.json", dict(state="first_batch_complete", pid=os.getpid(), updated_utc=utc(),
              phases=phases, latest_run=str(previous),
              next_step="review large-scale import results and certify remote/dynamic recall before formal runs"))
        print("[%s] CONTINUATION_COMPLETE" % utc(), flush=True)
        return 0
    except BaseException as error:
        if child and child.poll() is None:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
        write(root / "status.json", dict(state="stopped", pid=os.getpid(), updated_utc=utc(),
              error=str(error), phases=phases, evidence_preserved=True))
        print("STOPPED: " + str(error), file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
