import copy
import hashlib
import importlib.util
from pathlib import Path
import tempfile
import unittest

PATH = Path(__file__).parents[1] / "scripts/continue-high-recall-preparation.py"
SPEC = importlib.util.spec_from_file_location("continuation", PATH)
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


class ContinuationTests(unittest.TestCase):
    def test_cls_proof_must_cover_exactly_five_matching_osds(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "cls.so"
            binary.write_bytes(b"fixture module")
            digest = hashlib.sha256(binary.read_bytes()).hexdigest()
            proof = dict(status="pass", fsid="c83da116-1e8b-11f1-933d-cd48aa44d216",
                         expected_sha256=digest, osds=[dict(osd=i, sha256=digest, matches_expected=True)
                                                       for i in range(5)])
            MOD.validate_cls(proof, binary)
            for mutation in ("missing", "duplicate", "wrong-hash", "wrong-fsid"):
                value = copy.deepcopy(proof)
                if mutation == "missing": value["osds"].pop()
                elif mutation == "duplicate": value["osds"][-1]["osd"] = 0
                elif mutation == "wrong-hash": value["osds"][2]["sha256"] = "0" * 64
                else: value["fsid"] = "wrong"
                with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                    MOD.validate_cls(value, binary)
