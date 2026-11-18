import copy
import importlib.util
import json
from pathlib import Path
import unittest

PATH = Path(__file__).parents[1] / "scripts/baseline_cluster_guard.py"
SPEC = importlib.util.spec_from_file_location("cluster_guard", PATH)
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


class GuardTests(unittest.TestCase):
    def row(self, cursor, message="ordinary completed transaction"):
        return dict(__CURSOR=cursor, _BOOT_ID="boot", _SYSTEMD_INVOCATION_ID="daemon", MESSAGE=message)

    def test_inclusive_anchor_and_new_records_advance(self):
        data = "\n".join(json.dumps(self.row(x)) for x in ("a", "b", "c"))
        self.assertEqual(MOD.consume_journal(data, "a", "boot", "daemon"), ("c", [], 2))

    def test_same_health_code_still_detects_a_new_slow_event(self):
        data = "\n".join(json.dumps(x) for x in
                         (self.row("a"), self.row("b", "slow aio_wait, latency = 7s")))
        cursor, events, count = MOD.consume_journal(data, "a", "boot", "daemon")
        self.assertEqual((cursor, count, len(events)), ("b", 1, 1))

    def test_missing_cursor_and_restart_cannot_skip_evidence(self):
        for text in ("", json.dumps(self.row("other"))):
            with self.assertRaises(RuntimeError):
                MOD.consume_journal(text, "a", "boot", "daemon")
        changed = self.row("b"); changed["_SYSTEMD_INVOCATION_ID"] = "new-daemon"
        with self.assertRaisesRegex(RuntimeError, "restarted"):
            MOD.consume_journal(json.dumps(self.row("a")) + "\n" + json.dumps(changed),
                                "a", "boot", "daemon")

    def test_new_affected_osd_and_muting_are_rejected(self):
        document = dict(checks=dict(BLUESTORE_SLOW_OP_ALERT=dict(
            detail=[dict(message="osd.2 observed slow operations")], muted=False)))
        self.assertEqual(MOD.validate_health_detail(document), [2])
        for change in (dict(message="osd.3 observed slow operations"), dict(message="unknown target")):
            bad = copy.deepcopy(document)
            bad["checks"]["BLUESTORE_SLOW_OP_ALERT"]["detail"].append(change)
            with self.assertRaises(RuntimeError): MOD.validate_health_detail(bad)
        document["checks"]["BLUESTORE_SLOW_OP_ALERT"]["muted"] = True
        with self.assertRaises(RuntimeError): MOD.validate_health_detail(document)

    def test_new_device_errors_and_restart_messages_are_events(self):
        for message in ("Input/output error", "I/O error", "Stopped Ceph OSD", "Main process exited"):
            text = json.dumps(self.row("a")) + "\n" + json.dumps(self.row("b", message))
            self.assertEqual(len(MOD.consume_journal(text, "a", "boot", "daemon")[1]), 1)
