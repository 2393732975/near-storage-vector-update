import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "scripts/auto-resume-high-recall-formal.py"
SPEC = importlib.util.spec_from_file_location("formal_auto_resume", SCRIPT)
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)

ACTIVE_OSDS = {0, 2, 3, 4}
GUARD_STOP = "New OSD slow, restart or device events during formal work"
BLUESTORE = (
    "debug 2026-11-23T04:49:30.648+0000 7f61df910640  0 "
    "bluestore(/var/lib/ceph/osd/ceph-2) "
)
AIO_WAIT = "_txc_state_proc slow aio_wait, txc = 0x55732f390900, latency = 6.800926685s"
KV_COMMITTED = (
    "log_latency_fn slow operation observed for _txc_committed_kv, "
    "latency = 6.884790897s, txc = 0x55732f390900"
)


def event(message, osd=2, timestamp_us=1795409370652654):
    return {"osd": osd, "timestamp_us": timestamp_us, "message": message}


def anchors(label):
    return {osd: {"cursor": f"{label}-{osd}", "boot": "boot", "invocation": f"daemon-{osd}"}
            for osd in ACTIVE_OSDS}


class FakeGuard:
    def __init__(self, root):
        self.recovery_source = root
        self.anchors = {}
        self._alert_ids = mock.Mock(return_value=set())
        self._record = mock.Mock()


class FormalAutoResumeTests(unittest.TestCase):
    def test_recognizes_observed_aio_and_kv_records(self):
        events = [
            event(BLUESTORE + AIO_WAIT),
            event(BLUESTORE + KV_COMMITTED, timestamp_us=1795409370686824),
        ]
        self.assertTrue(MOD.slow_events(events, ACTIVE_OSDS))

    def test_recognizes_kv_commit_and_sync_without_transaction_id(self):
        prefix = (
            "debug 2026-11-22T10:16:05.554+0000 7f2128677640  0 "
            "bluestore(/var/lib/ceph/osd/ceph-3) log_latency slow operation observed for "
        )
        for operation in ("kv_commit", "kv_sync"):
            with self.subTest(operation=operation):
                row = event(prefix + operation + ", latency = 10.728136063s", osd=3)
                self.assertTrue(MOD.slow_events([row], ACTIVE_OSDS))

    def test_requires_known_pattern_and_matching_active_osd(self):
        invalid = {
            "different journal daemon": event(BLUESTORE + AIO_WAIT, osd=3),
            "excluded OSD": event(BLUESTORE.replace("ceph-2", "ceph-1") + AIO_WAIT, osd=1),
            "unknown slow operation": event(
                BLUESTORE + KV_COMMITTED.replace("_txc_committed_kv", "unexpected_operation")
            ),
            "bare keyword": event("slow aio_wait"),
            "invalid transaction": event(BLUESTORE + AIO_WAIT.replace("0x55732f390900", "broken")),
            "device error suffix": event(BLUESTORE + AIO_WAIT + ", Input/output error"),
        }
        self.assertFalse(MOD.slow_events([], ACTIVE_OSDS))
        for description, row in invalid.items():
            with self.subTest(description=description):
                self.assertFalse(MOD.slow_events([row], ACTIVE_OSDS))

    def test_rejects_device_or_restart_even_with_valid_slow_record(self):
        for message in ("Buffer I/O error on dev sdd", "Main process exited, status=1/FAILURE"):
            row = event(message)
            with self.subTest(message=message):
                self.assertFalse(MOD.slow_events([row], ACTIVE_OSDS))
                self.assertFalse(MOD.slow_events([event(BLUESTORE + AIO_WAIT), row], ACTIVE_OSDS))

    def test_recovers_only_guard_stop_with_valid_events(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = root / "evidence"
            evidence.mkdir()
            event_path = evidence / "ceph-guard-unexpected-events.json"
            events = [event(BLUESTORE + AIO_WAIT)]
            event_path.write_text(json.dumps(events), encoding="utf-8")
            status_path = root / "status.json"

            for status in (
                {"state": "running", "error": GUARD_STOP},
                {"state": "stopped", "error": "step timeout: deep100m-rep2-osd-import"},
                {"state": "stopped", "error": GUARD_STOP + "."},
            ):
                status_path.write_text(json.dumps(status), encoding="utf-8")
                with self.subTest(status=status), self.assertRaises(RuntimeError):
                    MOD.stopped_events(root, ACTIVE_OSDS)

            status_path.write_text(json.dumps({"state": "stopped", "error": GUARD_STOP}), encoding="utf-8")
            self.assertEqual(MOD.stopped_events(root, ACTIVE_OSDS), events)

            event_path.write_text(json.dumps(events + [event("Input/output error")]), encoding="utf-8")
            with self.assertRaises(RuntimeError):
                MOD.stopped_events(root, ACTIVE_OSDS)

    def fake_recovery_guard(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        recorded_stop = [event(BLUESTORE + AIO_WAIT), event(BLUESTORE + KV_COMMITTED)]
        prior = {
            "osds": sorted(ACTIVE_OSDS),
            "retained_slow_osds": [0],
            "journal_anchors": anchors("prior"),
        }
        (root / "ceph-guard-status.json").write_text(json.dumps(prior), encoding="utf-8")
        (root / "ceph-guard-unexpected-events.json").write_text(json.dumps(recorded_stop), encoding="utf-8")
        guard = MOD.recovery_guard(FakeGuard, ACTIVE_OSDS, mock.Mock())(root)
        quiet = {osd: {"read_eio": 0} for osd in ACTIVE_OSDS}
        guard._quiet_evidence = mock.Mock(return_value=[quiet, quiet])
        return guard, recorded_stop

    def test_clear_dashboard_still_reads_from_verified_journal_anchors(self):
        guard, stop = self.fake_recovery_guard()
        readings = {
            "prior-2": (anchors("stop"), stop, len(stop)),
            "stop-2": (anchors("quiet"), [], 3),
        }
        guard._read_journals = mock.Mock(side_effect=lambda: readings[guard.anchors[2]["cursor"]])

        guard.anchor_clean()

        self.assertEqual(guard._read_journals.call_args_list, [mock.call(), mock.call()])
        self.assertEqual(guard.anchors, anchors("quiet"))
        self.assertEqual(guard.checked, 5)
        self.assertEqual(guard._quiet_evidence.call_count, 2)
        guard._record.assert_called_once()

    def test_large_journal_keeps_original_anchor_and_checks_later_events(self):
        prior = anchors("prior")[2]

        def rows():
            for sequence in range(30002):
                yield json.dumps({
                    "__CURSOR": prior["cursor"] if sequence == 0 else str(sequence),
                    "_BOOT_ID": prior["boot"], "_SYSTEMD_INVOCATION_ID": prior["invocation"],
                    "__REALTIME_TIMESTAMP": sequence,
                    "MESSAGE": "slow" if sequence in (0, 30001) else "ordinary log",
                }).encode()

        osd, last, events, count = MOD.scan_journal(
            rows(), 2, prior, lambda row: row["MESSAGE"] == "slow")
        self.assertEqual((osd, last["cursor"], count), (2, "30001", 30001))
        self.assertEqual(events, [event("slow", timestamp_us=30001)])
        journal = rows()
        next(journal)
        with self.assertRaisesRegex(RuntimeError, "journal anchor missing"):
            MOD.scan_journal(journal, 2, prior, lambda row: False)

    def test_missing_anchor_or_event_mismatch_prevents_quiet_recovery(self):
        for fault in ("journal continuity", "missing stop record", "extra stop record"):
            with self.subTest(fault=fault):
                guard, stop = self.fake_recovery_guard()
                if fault == "journal continuity":
                    guard._read_journals = mock.Mock(side_effect=RuntimeError("osd.2 journal anchor missing"))
                else:
                    records = stop[:-1] if fault == "missing stop record" else stop + [stop[0]]
                    guard._read_journals = mock.Mock(return_value=(anchors("stop"), records, len(records)))

                with self.assertRaises(RuntimeError):
                    guard._recover({0, 2})

                self.assertEqual(guard.anchors, anchors("prior"))
                guard._quiet_evidence.assert_not_called()
                guard._record.assert_not_called()

    def test_new_events_during_quiet_window_prevent_recovery(self):
        guard, stop = self.fake_recovery_guard()
        extra = event(BLUESTORE + AIO_WAIT, timestamp_us=1795409370752654)
        guard._read_journals = mock.Mock(side_effect=[(anchors("stop"), stop, len(stop)),
                                                     (anchors("quiet"), [extra], 1)])

        with self.assertRaisesRegex(RuntimeError, "New OSD journal events"):
            guard._recover({0, 2})

        self.assertEqual(guard.anchors, anchors("stop"))
        guard._record.assert_not_called()

    def test_wait_for_quiet_retries_wrapped_busy_result(self):
        suite = mock.Mock()
        phase = mock.Mock()
        failure = RuntimeError("wrapped admission error")
        failure.__cause__ = MOD.RetryableRecovery("still busy")
        operation = mock.Mock(side_effect=[failure, "admitted"])
        with mock.patch.object(MOD.time, "monotonic", return_value=20), \
                mock.patch.object(MOD.time, "sleep") as sleep:
            self.assertEqual(MOD.wait_for_quiet(operation, suite, phase, 60), "admitted")

        self.assertEqual(operation.call_count, 2)
        suite.resource_gate.assert_called_once_with()
        phase.assert_called_once_with("waiting_for_quiet_osd_io", reason=str(failure))
        sleep.assert_called_once_with(15)

    def test_wait_for_quiet_propagates_fatal_unwrapped_and_expired_failures(self):
        expired = RuntimeError("wrapped admission error after deadline")
        expired.__cause__ = MOD.RetryableRecovery("still busy")
        for failure in (RuntimeError("OSD read I/O error"), MOD.RetryableRecovery("unwrapped busy"), expired):
            with self.subTest(failure=failure):
                operation = mock.Mock(side_effect=failure)
                suite = mock.Mock()
                phase = mock.Mock()
                with mock.patch.object(MOD.time, "monotonic", return_value=20), \
                        mock.patch.object(MOD.time, "sleep") as sleep, self.assertRaises(RuntimeError) as caught:
                    MOD.wait_for_quiet(operation, suite, phase, 0 if failure is expired else 60)

                self.assertIs(caught.exception, failure)
                operation.assert_called_once_with()
                suite.resource_gate.assert_not_called()
                phase.assert_not_called()
                sleep.assert_not_called()

    def test_supervisor_follows_only_verified_stopped_attempts(self):
        scenarios = (
            ("recovers", ("slow", "complete"), None),
            ("stops at cap", ("slow", "slow"), "Automatic recovery attempt limit reached"),
            ("rejects device event", ("device",), "not a recognized BlueStore slow operation"),
            ("propagates fatal error", ("fatal",), "container identity changed"),
        )
        for name, outcomes, error in scenarios:
            with self.subTest(scenario=name), tempfile.TemporaryDirectory() as directory:
                results = Path(directory).resolve()
                latest = results / "latest.json"
                previous = results / "original-run"
                template = results / "frozen-runner"
                latest.write_text(json.dumps({"status": str(previous / "status.json"),
                                              "runner_template": str(template)}), encoding="utf-8")
                remaining = iter(outcomes)

                def fake_attempt(formal, guard_module, template, previous, root, args):
                    outcome = next(remaining)
                    if outcome == "complete":
                        return
                    if outcome == "fatal":
                        raise RuntimeError("container identity changed")
                    message = BLUESTORE + AIO_WAIT if outcome == "slow" else "Input/output error"
                    evidence = root / "evidence"
                    evidence.mkdir()
                    (root / "status.json").write_text(
                        json.dumps({"state": "stopped", "error": GUARD_STOP}), encoding="utf-8")
                    (evidence / "ceph-guard-unexpected-events.json").write_text(
                        json.dumps([event(message)]), encoding="utf-8")
                    raise MOD.RetryableRecovery("OSD log guard stopped the attempt")

                formal = mock.Mock()
                guard_module = mock.Mock(OSD_HOSTS=ACTIVE_OSDS)
                with mock.patch.multiple(MOD, RESULTS=results, LATEST=latest), \
                        mock.patch.object(MOD, "load_formal_runner", return_value=(formal, guard_module)), \
                        mock.patch.object(MOD, "attempt", side_effect=fake_attempt) as attempt, \
                        mock.patch.object(MOD.sys, "argv", [str(SCRIPT), "--confirm-reset", "--max-attempts", "2"]), \
                        mock.patch.object(MOD.signal, "signal"), mock.patch("builtins.print"):
                    if error is None:
                        self.assertEqual(MOD.main(), 0)
                    else:
                        with self.assertRaisesRegex(RuntimeError, error):
                            MOD.main()

                self.assertEqual(attempt.call_count, len(outcomes))
                attempts = [call.args for call in attempt.call_args_list]
                self.assertTrue(all(args[2] == template for args in attempts))
                self.assertEqual(attempts[0][3], previous)
                if len(outcomes) == 2:
                    self.assertEqual(attempts[1][3], attempts[0][4])
                    self.assertNotEqual(attempts[1][4], attempts[0][4])
