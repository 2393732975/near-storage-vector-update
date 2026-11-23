import copy
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "scripts/index-pool-snapshot.py"
SPEC = importlib.util.spec_from_file_location("index_pool_snapshot", SCRIPT)
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


class ObjectNotFound(Exception):
    pass


def object_record(payload, omap, version):
    return {"bytes": len(payload), "version": version, "payload": payload, "omap": omap}


class FakeIoctx:
    def __init__(self, objects, snapshot=None):
        self.objects = copy.deepcopy(objects)
        self.snapshot = snapshot
        self.snap_rollback = mock.Mock(side_effect=self.rollback)
        self.set_namespace = mock.Mock()

    def stat(self, oid):
        if oid not in self.objects:
            raise ObjectNotFound(oid)
        return self.objects[oid]["bytes"], None

    def rollback(self, oid, name):
        if oid in self.snapshot.objects:
            restored = copy.deepcopy(self.snapshot.objects[oid])
            restored["version"] = 99
            self.objects[oid] = restored
        else:
            self.objects.pop(oid, None)


def inventory(ioctx, *args, **kwargs):
    return {oid: {"bytes": obj["bytes"], "version": obj["version"]} for oid, obj in ioctx.objects.items()}


class IndexPoolSnapshotTests(unittest.TestCase):
    def test_capture_keeps_checkpoint_incomplete_until_snapshot_is_verified(self):
        head = mock.Mock()
        head.list_snaps.return_value = []
        snap = SimpleNamespace(name="baseline", snap_id=8)
        head.lookup_snap.return_value = snap
        objects = {"node": {"bytes": 5, "version": 17}}
        document = {"state": "creating", "snapshot": "baseline", "placement": [],
                    "pools": {"nsvu_meta": {"objects": objects}}}
        snapshot = mock.Mock()
        snapshot.stat.return_value = (5, None)
        snapshot.get_last_version.return_value = 18
        cluster = mock.MagicMock()
        cluster.open_ioctx.return_value.__enter__.return_value = snapshot
        with tempfile.TemporaryDirectory() as folder, \
                mock.patch.object(MOD, "inventory", return_value=objects), \
                mock.patch.object(MOD, "pg_layout"):
            path = Path(folder) / "checkpoint.json"
            contexts = {"nsvu_meta": head}
            with self.assertRaisesRegex(RuntimeError, "Snapshot does not match"):
                MOD.capture(path, document, cluster, contexts, "fake-keyring")
            partial = json.loads(path.read_text())
            self.assertEqual(partial["state"], "creating")
            self.assertEqual(partial["pools"]["nsvu_meta"]["snap_id"], 8)

            head.list_snaps.return_value = [snap]
            snapshot.get_last_version.return_value = 17
            MOD.capture(path, partial, cluster, contexts, "fake-keyring")
            self.assertEqual(json.loads(path.read_text())["state"], "ready")
            head.create_snap.assert_called_once_with("baseline")

    def test_changed_objects_includes_versions_sizes_and_inventory_changes(self):
        baseline = {
            "size-changed": {"bytes": 1, "version": 3},
            "same-size-updated": {"bytes": 5, "version": 2},
            "deleted": {"bytes": 1, "version": 3},
            "unchanged": {"bytes": 1, "version": 3},
        }
        head = {
            "unchanged": {"bytes": 1, "version": 3},
            "same-size-updated": {"bytes": 5, "version": 7},
            "added": {"bytes": 1, "version": 5},
            "size-changed": {"bytes": 2, "version": 3},
        }
        self.assertEqual(MOD.changed_objects(baseline, head),
                         ["added", "deleted", "same-size-updated", "size-changed"])

    def pool_fixture(self):
        snapshot = FakeIoctx({
            "changed": object_record(b"graph", {"node:1": b"old"}, 1),
            "deleted": object_record(b"vector", {"vec:4": b"ref"}, 2),
            "unchanged": object_record(b"", {"label:2": b"id"}, 3),
        })
        head = FakeIoctx({
            "added": object_record(b"new", {}, 4),
            "changed": object_record(b"graph", {"node:1": b"new"}, 5),
            "unchanged": snapshot.objects["unchanged"],
        }, snapshot)
        digest = mock.Mock()
        digest.payload_digest.side_effect = lambda ioctx, oid, size: ioctx.objects[oid]["payload"].hex()
        digest.omap_digest.side_effect = lambda ioctx, oid: (
            tuple(sorted(ioctx.objects[oid]["omap"].items())), len(ioctx.objects[oid]["omap"]))
        return head, snapshot, {"objects": inventory(snapshot)}, digest

    def test_restores_changed_and_deleted_objects_and_removes_new_head(self):
        head, snapshot, pool, digest = self.pool_fixture()
        with mock.patch.object(MOD, "inventory", side_effect=inventory):
            restored = MOD.restore_pool(head, snapshot, pool, "baseline", digest, ObjectNotFound)

        self.assertEqual(restored, 3)
        self.assertEqual(head.snap_rollback.call_args_list,
                         [mock.call(oid, "baseline") for oid in ("added", "changed", "deleted")])
        self.assertEqual(set(head.objects), set(snapshot.objects))
        self.assertEqual(head.objects["changed"]["omap"], {"node:1": b"old"})
        self.assertEqual(head.objects["deleted"]["payload"], b"vector")
        self.assertEqual(head.objects["unchanged"]["version"], 3)
        digest.omap_digest.assert_any_call(head, "changed")
        digest.omap_digest.assert_any_call(snapshot, "changed")

    def test_omap_mismatch_leaves_checkpoint_unready_until_verified_retry(self):
        head, snapshot, pool, digest = self.pool_fixture()
        snapshot.set_read = mock.Mock()
        cluster = mock.MagicMock()
        cluster.open_ioctx.return_value.__enter__.return_value = snapshot
        rados = mock.Mock(ObjectNotFound=ObjectNotFound)
        name = "nsvu_owner_0"
        document = {
            "state": "ready",
            "snapshot": "baseline",
            "placement": [],
            "pools": {name: {**pool, "snap_id": 8}},
        }

        def corrupt_rollback(oid, name):
            head.rollback(oid, name)
            if oid == "changed":
                head.objects[oid]["omap"]["node:1"] = b"wrong"

        head.snap_rollback.side_effect = corrupt_rollback
        with tempfile.TemporaryDirectory() as folder, \
                mock.patch.object(MOD, "inventory", side_effect=inventory), \
                mock.patch.object(MOD, "load_digest", return_value=digest), \
                mock.patch.object(MOD, "pg_layout") as pg_layout:
            checkpoint = Path(folder) / "checkpoint.json"
            with self.assertRaisesRegex(RuntimeError, "Restored data or OMAP differs"):
                MOD.restore(checkpoint, document, cluster, {name: head}, rados, "fake-keyring")

            saved = json.loads(checkpoint.read_text(encoding="utf-8"))
            self.assertEqual(saved["state"], "restoring")
            self.assertEqual(saved["pools"][name]["objects"], pool["objects"])
            pg_layout.assert_not_called()

            head.snap_rollback.side_effect = head.rollback
            report = MOD.restore(checkpoint, saved, cluster, {name: head}, rados, "fake-keyring")
            self.assertEqual(report["objects_rolled_back"], {name: 2})
            saved = json.loads(checkpoint.read_text(encoding="utf-8"))
            self.assertEqual(saved["state"], "ready")
            self.assertEqual(saved["pools"][name]["objects"], inventory(head))
            pg_layout.assert_called_once_with("fake-keyring", [])
