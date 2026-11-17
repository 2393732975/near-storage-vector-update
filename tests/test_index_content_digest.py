import hashlib
import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location(
    "index_content_digest", Path(__file__).parents[1] / "scripts/index-content-digest.py")
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


class Object:
    def __init__(self, data, short=False):
        self.data, self.short = data, short

    def read(self, oid, length, offset):
        return self.data[offset:offset + length - int(self.short)]


class DigestTests(unittest.TestCase):
    def test_chunking_does_not_change_payload_identity(self):
        data = b"x\x00abc" * 17
        for block in (1, 7, 1024):
            self.assertEqual(MOD.payload_digest(Object(data), "oid", len(data), block),
                             hashlib.sha256(data).hexdigest())

    def test_short_read_cannot_certify_identity(self):
        with self.assertRaises(RuntimeError):
            MOD.payload_digest(Object(b"abcdefgh", short=True), "oid", 8, 4)

    def test_key_value_boundaries_are_part_of_identity(self):
        left, right = hashlib.sha256(), hashlib.sha256()
        for value in (b"a", b"bc"):
            MOD.framed(left, value)
        for value in (b"ab", b"c"):
            MOD.framed(right, value)
        self.assertNotEqual(left.hexdigest(), right.hexdigest())
