import copy
import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

PATH = Path(__file__).parents[1] / "scripts/run-high-recall-preparation.py"
SPEC = importlib.util.spec_from_file_location("preparation", PATH)
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


class PreparationTests(unittest.TestCase):
    def gate(self, directory, status=None, nodes=None):
        suite = object.__new__(MOD.Suite)
        suite.args = mock.Mock(keyring=Path("/not/read"))
        suite.root = Path(directory)
        suite.allowed_health = {"MGR_MODULE_ERROR", "POOL_NO_REDUNDANCY", "LARGE_OMAP_OBJECTS"}
        document = status if status is not None else dict(
            osdmap=dict(num_osds=5, num_up_osds=5, num_in_osds=5),
            pgmap=dict(num_pgs=39, pgs_by_state=[dict(state_name="active+clean", count=39)]),
            health=dict(checks={}))
        capacities = nodes if nodes is not None else [dict(id=i, utilization=5) for i in range(5)]
        suite.ceph_json = lambda *cmd: copy.deepcopy(document if cmd == ("status",)
                                                    else dict(nodes=capacities))
        return suite, document

    def test_reset_confirmation_is_required_before_any_setup(self):
        result = subprocess.run(["python3", str(PATH), "--run-root", "/must-not-create"],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("--confirm-reset is required", result.stderr)

    def test_missing_or_inconsistent_pg_evidence_fails_closed(self):
        with tempfile.TemporaryDirectory() as folder:
            for pgmap in [dict(num_pgs=39, pgs_by_state=[]),
                          dict(num_pgs=39, pgs_by_state=[dict(state_name="active+clean", count=38)]),
                          dict(num_pgs=39, pgs_by_state=[dict(state_name="active+degraded", count=39)])]:
                suite, doc = self.gate(folder)
                doc["pgmap"] = pgmap
                with mock.patch.object(MOD.subprocess, "check_output", return_value=MOD.FSID), \
                        self.assertRaisesRegex(RuntimeError, "PGs"):
                    suite.cluster_gate()

    def test_capacity_and_manager_error_changes_fail_closed(self):
        with tempfile.TemporaryDirectory() as folder:
            suite, _ = self.gate(folder, nodes=[])
            with mock.patch.object(MOD.subprocess, "check_output", return_value=MOD.FSID), \
                    self.assertRaisesRegex(RuntimeError, "capacity report"):
                suite.cluster_gate()
            suite, doc = self.gate(folder)
            doc["health"]["checks"]["MGR_MODULE_ERROR"] = dict(
                summary=dict(message="another module has failed"))
            with mock.patch.object(MOD.subprocess, "check_output", return_value=MOD.FSID), \
                    self.assertRaisesRegex(RuntimeError, "manager module"):
                suite.cluster_gate()

    def test_wrong_fsid_is_rejected_before_status_query(self):
        with tempfile.TemporaryDirectory() as folder:
            suite, _ = self.gate(folder)
            suite.ceph_json = mock.Mock(side_effect=AssertionError("must not query other cluster"))
            with mock.patch.object(MOD.subprocess, "check_output", return_value="wrong"), \
                    self.assertRaisesRegex(RuntimeError, "FSID"):
                suite.cluster_gate()
