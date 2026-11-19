import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).parents[1] / "scripts/initialize-experiment-pools.sh"

FAKE_CEPH = r'''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
if args[:1] == ["--keyring"]:
    args = args[2:]
with Path(os.environ["FAKE_LOG"]).open("a") as f:
    f.write(json.dumps(dict(tool="ceph", args=args)) + "\n")
path = Path(os.environ["FAKE_STATE"])
state = json.loads(path.read_text())
if args == ["fsid"]:
    print(state["fsid"])
elif args == ["osd", "dump", "--format", "json"]:
    print(json.dumps(dict(osds=state["osds"])))
elif args == ["osd", "pool", "ls"]:
    print("\n".join(sorted(state["pools"])))
elif args[:3] == ["osd", "pool", "rm"]:
    assert args[3] == args[4]
    del state["pools"][args[3]]
elif args[:3] == ["osd", "pool", "create"]:
    state["pools"][args[3]] = dict(id=state["next_id"], acting=0,
        pg_num="1", pg_autoscale_mode="off")
    state["next_id"] += 1
elif args[:3] == ["osd", "pool", "set"]:
    state["pools"][args[3]][args[4]] = args[5]
elif args[:4] == ["osd", "pool", "application", "enable"]:
    assert args[4] in state["pools"]
elif args[:3] == ["osd", "pool", "get"]:
    print(args[4] + ": " + state["pools"][args[3]][args[4]])
elif args == ["osd", "lspools"]:
    for name, pool in state["pools"].items():
        print(pool["id"], name)
elif args[:2] == ["pg", "map"]:
    pool = next(p for p in state["pools"].values() if p["id"] == int(args[2].split(".")[0]))
    print("osdmap e1 pg %s (%s) -> up [%d] acting [%d]" %
        (args[2], args[2], pool["acting"], pool["acting"]))
elif args[:2] == ["osd", "pg-upmap-items"]:
    pool = next(p for p in state["pools"].values() if p["id"] == int(args[2].split(".")[0]))
    assert pool["acting"] == int(args[3])
    pool["acting"] = int(args[4])
else:
    raise SystemExit("unexpected fake Ceph command: " + repr(args))
path.write_text(json.dumps(state))
'''

FAKE_RADOS = r'''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
with Path(os.environ["FAKE_LOG"]).open("a") as f:
    f.write(json.dumps(dict(tool="rados", args=sys.argv[1:])) + "\n")
'''


class ExperimentPoolMappingTests(unittest.TestCase):
    def invoke(self, folder, overrides=None, osds=None, fsid="test-fsid"):
        root = Path(folder)
        fake_ceph, fake_rados = root / "ceph", root / "rados"
        fake_ceph.write_text(FAKE_CEPH)
        fake_rados.write_text(FAKE_RADOS)
        fake_ceph.chmod(0o700)
        fake_rados.chmod(0o700)
        initial = dict(fsid=fsid, osds=osds if osds is not None else [
            dict(osd=i, up=int(i != 1), **{"in": int(i != 1)}) for i in range(5)],
            pools={name: dict(id=i + 1, acting=0, pg_num="1", pg_autoscale_mode="off")
                   for i, name in enumerate([".mgr", "rbd", "nsvu_meta"] +
                                            ["nsvu_owner_%d" % n for n in range(5)])},
            next_id=100)
        state, log = root / "state.json", root / "calls.jsonl"
        state.write_text(json.dumps(initial))
        log.write_text("")
        env = dict(os.environ)
        for name in ("OWNERS", "OWNER_OSDS", "META_POOL", "OWNER_POOL_PREFIX", "CEPH_KEYRING"):
            env.pop(name, None)
        env.update(CEPH_CMD=str(fake_ceph), RADOS_CMD=str(fake_rados),
                   FAKE_STATE=str(state), FAKE_LOG=str(log), EXPECTED_CEPH_FSID="test-fsid")
        env.update(overrides or {})
        result = subprocess.run(["bash", str(SCRIPT), "--confirm-reset"], env=env,
                                capture_output=True, text=True)
        calls = [json.loads(line) for line in log.read_text().splitlines()]
        return result, initial, json.loads(state.read_text()), calls

    def assert_no_mutations(self, calls):
        for call in calls:
            self.assertEqual(call["tool"], "ceph")
            self.assertIn(call["args"], (["fsid"], ["osd", "dump", "--format", "json"]))

    def test_four_owners_map_to_noncontiguous_online_osds(self):
        with tempfile.TemporaryDirectory() as folder:
            result, before, after, calls = self.invoke(folder, dict(OWNERS="4", OWNER_OSDS="0,2,3,4"))
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(after["pools"]["nsvu_meta"]["acting"], 0)
            for owner, osd in enumerate((0, 2, 3, 4)):
                self.assertEqual(after["pools"]["nsvu_owner_%d" % owner]["acting"], osd)
            for name in (".mgr", "rbd", "nsvu_owner_4"):
                self.assertEqual(before["pools"][name], after["pools"][name])
            puts = [x for x in calls if x["tool"] == "rados"]
            self.assertEqual(len(puts), 5)
            self.assertTrue(all("nsvu_owner_4" not in x["args"] for x in puts))
            self.assertEqual(calls[:2], [dict(tool="ceph", args=["fsid"]),
                                        dict(tool="ceph", args=["osd", "dump", "--format", "json"])])

    def test_meta_uses_first_osd_and_whitespace_mapping_is_supported(self):
        with tempfile.TemporaryDirectory() as folder:
            result, _, after, _ = self.invoke(folder, dict(OWNERS="2", OWNER_OSDS="3 \n4"))
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(after["pools"]["nsvu_meta"]["acting"], 3)
            self.assertEqual(after["pools"]["nsvu_owner_0"]["acting"], 3)
            self.assertEqual(after["pools"]["nsvu_owner_1"]["acting"], 4)

    def test_default_five_mapping_is_preserved_when_every_target_is_up_in(self):
        with tempfile.TemporaryDirectory() as folder:
            osds = [dict(osd=i, up=1, **{"in": 1}) for i in range(5)]
            result, _, after, _ = self.invoke(folder, osds=osds)
            self.assertEqual(result.returncode, 0, result.stderr)
            for owner in range(5):
                self.assertEqual(after["pools"]["nsvu_owner_%d" % owner]["acting"], owner)

    def test_invalid_mapping_or_pool_scope_fails_before_any_ceph_call(self):
        cases = [dict(OWNERS="4", OWNER_OSDS=value) for value in
                 ("0,2,3", "0,2,3,4,5", "0,2,2,4", "0,2,02,4", "0,2,-3,4",
                  "0,2,three,4", "0,,3,4", ",0,2,3,4", "0,2,3,4,", "", "   ",
                  "0,2,3,2147483648")]
        cases += [dict(OWNERS=value) for value in ("0", "6", "2x", "-1")]
        cases += [dict(META_POOL="rbd"), dict(META_POOL=".mgr"), dict(OWNER_POOL_PREFIX="rbd")]
        for overrides in cases:
            with self.subTest(overrides=overrides), tempfile.TemporaryDirectory() as folder:
                result, before, after, calls = self.invoke(folder, overrides)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(calls, [])
                self.assertEqual(after, before)

    def test_down_out_or_missing_target_refuses_reset_before_mutations(self):
        for bad in (dict(up=0, **{"in": 1}), dict(up=1, **{"in": 0}), None):
            with self.subTest(target_state=bad), tempfile.TemporaryDirectory() as folder:
                osds = [dict(osd=i, up=1, **{"in": 1}) for i in (0, 2, 3)]
                if bad is not None:
                    osds.append(dict(osd=4, **bad))
                result, before, after, calls = self.invoke(
                    folder, dict(OWNERS="4", OWNER_OSDS="0,2,3,4"), osds=osds)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("must be present and up/in", result.stderr)
                self.assert_no_mutations(calls)
                self.assertEqual(after, before)

    def test_old_default_cannot_silently_include_disabled_osd(self):
        with tempfile.TemporaryDirectory() as folder:
            result, before, after, calls = self.invoke(folder)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("osd.1", result.stderr)
            self.assert_no_mutations(calls)
            self.assertEqual(after, before)

    def test_wrong_fsid_or_duplicate_osd_state_refuses_before_mutations(self):
        for fsid, osds in [("other-fsid", None), ("test-fsid", [
                dict(osd=i, up=1, **{"in": 1}) for i in (0, 2, 3, 4, 4)])]:
            with self.subTest(fsid=fsid), tempfile.TemporaryDirectory() as folder:
                result, before, after, calls = self.invoke(
                    folder, dict(OWNERS="4", OWNER_OSDS="0,2,3,4"), osds=osds, fsid=fsid)
                self.assertNotEqual(result.returncode, 0)
                self.assert_no_mutations(calls)
                self.assertEqual(after, before)


if __name__ == "__main__":
    unittest.main()
