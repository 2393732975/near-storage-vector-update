import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).parents[1] / "scripts/cleanup-experiment-pools.sh"
FAKE = r'''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
state_path = Path(os.environ['FAKE_STATE'])
state = json.loads(state_path.read_text())
args = sys.argv[1:]
with Path(os.environ['FAKE_LOG']).open('a') as f:
    f.write(json.dumps(args) + '\n')
if args == ['fsid']:
    print(state['fsid'])
elif args == ['osd', 'pool', 'ls']:
    print('\n'.join(state['pools']))
elif args == ['osd', 'dump', '--format', 'json']:
    print(json.dumps(dict(fsid=state['fsid'], osds=state['osds'])))
elif args == ['osd', 'stat']:
    print('5 osds: 4 up, 4 in')
elif args == ['pg', 'stat']:
    print(state.get('pg_state', '33 pgs: 33 active+clean'))
elif args[:3] == ['osd', 'pool', 'rm']:
    state['pools'].remove(args[3])
    state_path.write_text(json.dumps(state))
elif args in (['-s'], ['health']):
    print('HEALTH_OK')
else:
    raise SystemExit('Unexpected fake command: ' + repr(args))
'''


class CleanupLayoutTests(unittest.TestCase):
    def invoke(self, folder, extra=None, change=None, dry=False):
        root = Path(folder)
        ceph = root / "ceph"
        ceph.write_text(FAKE)
        ceph.chmod(0o700)
        state = dict(fsid="test-fsid", osds=[dict(osd=i, up=int(i != 1), **{"in": int(i != 1)})
                                           for i in range(5)],
                     pools=[".mgr", "rbd", "nsvu_meta"] + ["nsvu_owner_%d" % i for i in range(5)])
        if change:
            change(state)
        state_path, log = root / "state.json", root / "calls.jsonl"
        state_path.write_text(json.dumps(state))
        log.write_text("")
        env = dict(os.environ)
        for name in ["CEPH_KEYRING", "EXPECTED_ACTIVE_OSDS", "EXPECTED_EXCLUDED_OSDS"]:
            env.pop(name, None)
        env.update(CEPH_CMD=str(ceph), CEPH_USE_SUDO="false", OWNERS="5",
                   EXPECTED_CEPH_FSID="test-fsid", FAKE_STATE=str(state_path), FAKE_LOG=str(log))
        env.update(extra or {})
        result = subprocess.run(["bash", str(SCRIPT)] + ([] if dry else ["--confirm-cleanup"]),
                                capture_output=True, text=True, env=env)
        calls = [json.loads(line) for line in log.read_text().splitlines()]
        return result, json.loads(state_path.read_text()), calls

    def test_declared_four_online_osds_clean_only_authorized_pools(self):
        with tempfile.TemporaryDirectory() as folder:
            result, state, calls = self.invoke(folder, dict(EXPECTED_ACTIVE_OSDS="0,2,3,4",
                                                           EXPECTED_EXCLUDED_OSDS="1"))
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(state["pools"], [".mgr", "rbd"])
            deleted = [call[3] for call in calls if call[:3] == ["osd", "pool", "rm"]]
            self.assertEqual(set(deleted), {"nsvu_meta"} | {"nsvu_owner_%d" % i for i in range(5)})

    def test_default_cleanup_still_refuses_a_down_out_osd(self):
        with tempfile.TemporaryDirectory() as folder:
            result, _, calls = self.invoke(folder)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(any(call[:3] == ["osd", "pool", "rm"] for call in calls))

    def test_explicit_layout_rejects_wrong_active_or_excluded_state(self):
        for osd, field, value in [(1, "up", 1), (1, "in", 1), (2, "up", 0), (4, "in", 0)]:
            with self.subTest(osd=osd, field=field), tempfile.TemporaryDirectory() as folder:
                def change(state):
                    state["osds"][osd][field] = value
                result, _, calls = self.invoke(folder, dict(EXPECTED_ACTIVE_OSDS="0,2,3,4",
                                                           EXPECTED_EXCLUDED_OSDS="1"), change)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(any(call[:3] == ["osd", "pool", "rm"] for call in calls))

    def test_duplicate_or_overlapping_layout_cannot_delete(self):
        for active in ["0,2,2,4", "0,1,2,3,4", "0,2,3,9", "0,2,-3,4"]:
            with self.subTest(active=active), tempfile.TemporaryDirectory() as folder:
                result, _, calls = self.invoke(folder, dict(EXPECTED_ACTIVE_OSDS=active,
                                                           EXPECTED_EXCLUDED_OSDS="1"))
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(any(call[:3] == ["osd", "pool", "rm"] for call in calls))

    def test_nonclean_pg_or_wrong_fsid_cannot_delete(self):
        for change in [lambda state: state.update(pg_state="39 pgs: 38 active+clean, 1 peering"),
                       lambda state: state.update(fsid="other-fsid")]:
            with tempfile.TemporaryDirectory() as folder:
                result, _, calls = self.invoke(folder, dict(EXPECTED_ACTIVE_OSDS="0,2,3,4",
                                                           EXPECTED_EXCLUDED_OSDS="1"), change)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(any(call[:3] == ["osd", "pool", "rm"] for call in calls))

    def test_dry_run_never_deletes(self):
        with tempfile.TemporaryDirectory() as folder:
            result, state, calls = self.invoke(folder, dry=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(len(state["pools"]), 8)
            self.assertFalse(any(call[:3] == ["osd", "pool", "rm"] for call in calls))
