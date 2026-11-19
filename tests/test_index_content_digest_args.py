import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock


PATH = Path(__file__).parents[1] / "scripts/index-content-digest.py"
SPEC = importlib.util.spec_from_file_location("digest_with_owners", PATH)
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


class Context:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class FakeIoctx(Context):
    def list_objects(self):
        return [SimpleNamespace(key="one-object")]

    def stat(self, oid):
        return (0, None)

    def get_omap_vals(self, op, cursor, prefix, count):
        return (iter([]), 0)

    def operate_read_op(self, op, oid):
        pass


class FakeCluster(Context):
    def __init__(self, fsid):
        self.fsid, self.opened = fsid, []

    def get_fsid(self):
        return self.fsid

    def open_ioctx(self, name):
        self.opened.append(name)
        return FakeIoctx()


class IndexDigestOwnerArgumentTests(unittest.TestCase):
    def invoke(self, root, extra=(), fsid="test-fsid"):
        output = root / "digest.json"
        cluster = FakeCluster(fsid)
        factory = mock.Mock(return_value=cluster)
        fake = SimpleNamespace(Rados=factory, ReadOpCtx=Context)
        args = [str(PATH), "--keyring", "/not/read", "--expected-fsid", "test-fsid",
                "--output", str(output), *extra]
        with mock.patch.object(sys, "argv", args), mock.patch.dict(sys.modules, rados=fake), \
                contextlib.redirect_stdout(io.StringIO()):
            MOD.main()
        return json.loads(output.read_text()), cluster, factory

    def test_four_owner_digest_never_opens_retired_fifth_pool(self):
        with tempfile.TemporaryDirectory() as folder:
            doc, cluster, _ = self.invoke(Path(folder), ["--owners", "4"])
            self.assertEqual(cluster.opened, ["nsvu_meta"] + ["nsvu_owner_%d" % i for i in range(4)])
            self.assertEqual(doc["owners"], 4)
            self.assertEqual(len(doc["objects"]), 5)
            self.assertEqual({x["pool"] for x in doc["objects"]}, set(cluster.opened))

    def test_default_five_owners_retains_legacy_records_and_hash_scope(self):
        with tempfile.TemporaryDirectory() as folder:
            doc, cluster, _ = self.invoke(Path(folder))
            self.assertEqual(cluster.opened, ["nsvu_meta"] + ["nsvu_owner_%d" % i for i in range(5)])
            self.assertEqual(doc["owners"], 5)
            empty = hashlib.sha256(b"").hexdigest()
            legacy_records = [dict(pool=p, object="one-object", bytes=0,
                                   payload_sha256=empty, omap_sha256=empty, omap_entries=0)
                              for p in cluster.opened]
            self.assertEqual(doc["objects"], legacy_records)
            expected = hashlib.sha256(json.dumps(legacy_records, sort_keys=True,
                                                separators=(",", ":")).encode()).hexdigest()
            self.assertEqual(doc["logical_sha256"], expected)

    def test_invalid_owner_count_is_rejected_before_cluster_connection(self):
        for value in ("0", "-1", "four", "1.5"):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as folder:
                output = Path(folder) / "must-not-exist.json"
                factory = mock.Mock(side_effect=AssertionError("must not connect"))
                args = [str(PATH), "--keyring", "/not/read", "--expected-fsid", "test-fsid",
                        "--output", str(output), "--owners", value]
                with mock.patch.object(sys, "argv", args), \
                        mock.patch.dict(sys.modules, rados=SimpleNamespace(Rados=factory)), \
                        contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                    MOD.main()
                self.assertEqual(error.exception.code, 2)
                factory.assert_not_called()
                self.assertFalse(output.exists())

    def test_fsid_failure_happens_before_any_pool_read(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(RuntimeError, "FSID mismatch"):
                self.invoke(Path(folder), ["--owners", "4"], fsid="other-fsid")
            self.assertFalse((Path(folder) / "digest.json").exists())


if __name__ == "__main__":
    unittest.main()
