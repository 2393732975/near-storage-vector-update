"""Admit a diagnosed retained warning while stopping on every new slow event.

This module only reads Ceph/journald. No health mute or configuration change is
performed. The retained-warning exception is restricted to the local osd.2.
"""

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess

FSID = "c83da116-1e8b-11f1-933d-cd48aa44d216"
UNIT = "ceph-" + FSID + "@osd.2.service"
EVENT = re.compile(r"slow operation observed|slow aio_wait|slow kv|I/O error|"
                   r"Input/output error|io_error|stalled|stuck", re.IGNORECASE)
RESTART = re.compile(r"^(Starting|Started|Stopping|Stopped) |Main process exited|"
                     r"Scheduled restart job|Failed with result", re.IGNORECASE)


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def validate_evidence(document):
    if (document.get("fsid") != FSID or document.get("known_slow_osds") != [2] or
            document.get("unit") != UNIT or not document.get("read_only_diagnostic") or
            document.get("configuration_changed") is not False or
            not document.get("journal_cursor") or not document.get("journal_boot_id") or
            not document.get("journal_systemd_invocation_id")):
        raise RuntimeError("retained slow-alert evidence identity is incomplete")
    samples = document.get("samples", [])
    if len(samples) < 2 or not document.get("quiet_samples_stable"):
        raise RuntimeError("retained slow-alert evidence lacks quiet samples")
    for key in ("num_ops", "op_wip", "txc_mem_items", "finisher_queue", "read_eio"):
        if any(row.get(key) != 0 for row in samples):
            raise RuntimeError("retained slow-alert admission is not idle/error-free")
    for key in ("osd_op", "osd_op_w", "txc_count", "kv_commit_count", "kv_commit_sum",
                "aio_wait_count", "aio_wait_sum"):
        values = [row.get(key) for row in samples]
        if values[0] is None or any(value != values[0] for value in values):
            raise RuntimeError("retained slow-alert counters are not stable")


def consume_journal(text, cursor, boot, invocation):
    if len(text.encode()) > 32 * 1024 * 1024:
        raise RuntimeError("slow-event journal window exceeds capture budget")
    rows = [json.loads(line) for line in text.splitlines() if line.strip()]
    if not rows or rows[0].get("__CURSOR") != cursor:
        raise RuntimeError("slow-event journal anchor missing or rotated away")
    if len(rows) > 20000:
        raise RuntimeError("slow-event journal record budget exceeded")
    events = []
    for row in rows:
        if (not row.get("__CURSOR") or row.get("_BOOT_ID") != boot or
                row.get("_SYSTEMD_INVOCATION_ID") != invocation):
            raise RuntimeError("OSD journal identity changed or daemon restarted")
    for row in rows[1:]:
        message = row.get("MESSAGE")
        if not isinstance(message, str):
            raise RuntimeError("invalid OSD journal message")
        if EVENT.search(message) or RESTART.search(message):
            events.append({key: row.get(key) for key in
                           ("__CURSOR", "__REALTIME_TIMESTAMP", "MESSAGE")})
    return rows[-1]["__CURSOR"], events, len(rows) - 1


def validate_health_detail(document):
    slow = document.get("checks", {}).get("BLUESTORE_SLOW_OP_ALERT")
    if slow is None:
        return []
    if slow.get("muted"):
        raise RuntimeError("slow-operation warning must remain visible")
    details = slow.get("detail", [])
    if not details:
        raise RuntimeError("slow-operation warning lacks affected OSD identities")
    affected = set()
    for detail in details:
        found = re.findall(r"\bosd\.(\d+)\b", detail.get("message", ""))
        if not found:
            raise RuntimeError("unrecognized slow-operation detail")
        affected.update(int(value) for value in found)
    if affected != {2}:
        raise RuntimeError("slow-operation warning expanded beyond diagnosed osd.2")
    return sorted(affected)


class RetainedSlowAlertGuard:
    def __init__(self, evidence_path, run_root, keyring):
        raw = Path(evidence_path).read_bytes()
        self.evidence = json.loads(raw)
        validate_evidence(self.evidence)
        self.root, self.keyring = Path(run_root), str(keyring)
        self.cursor = self.evidence["journal_cursor"]
        self.evidence_sha256 = hashlib.sha256(raw).hexdigest()
        self.records_checked = 0
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "retained-slow-evidence.json").write_bytes(raw)

    def check(self):
        # Include the previous anchor and verify it. Missing/vacuumed cursors
        # cannot silently skip an unobserved interval.
        result = subprocess.run(
            ["sudo", "-n", "journalctl", "-u", UNIT, "--cursor", self.cursor,
             "--no-pager", "-o", "json", "--quiet"],
            capture_output=True, text=True, timeout=30)
        if result.returncode or result.stderr.strip():
            raise RuntimeError("cannot verify slow-event journal interval")
        cursor, events, count = consume_journal(
            result.stdout, self.cursor, self.evidence["journal_boot_id"],
            self.evidence["journal_systemd_invocation_id"])
        if events:
            write_json(self.root / "new-slow-or-device-events.json", events)
            raise RuntimeError("new OSD slow/device event observed after retained-warning baseline")
        detail = json.loads(subprocess.check_output(
            ["/usr/bin/ceph", "--keyring", self.keyring, "health", "detail", "--format", "json"],
            timeout=30))
        affected = validate_health_detail(detail)
        self.cursor = cursor
        self.records_checked += count
        write_json(self.root / "slow-alert-guard.json", dict(
            checked_utc=datetime.now(timezone.utc).isoformat(),
            evidence_sha256=self.evidence_sha256, cursor=self.cursor,
            journal_unit=UNIT, records_checked=self.records_checked,
            retained_slow_osds=affected, new_events=0,
            scope="local osd.2 journal plus affected OSD identities in cluster health",
            configuration_changed=False, health_warning_muted=False))
        return detail
