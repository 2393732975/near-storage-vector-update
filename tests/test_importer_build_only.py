"""Offline importer smoke tests: these never connect to Ceph."""

import hashlib
import json
import os
from pathlib import Path
import random
import struct
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
BINARY = Path(os.environ.get("NSVU_IMPORTER_TEST_BINARY", ROOT / "build/nsvu-base-importer"))


@unittest.skipUnless(BINARY.is_file(), "build the importer to run offline smoke tests")
class OfflineImporterTests(unittest.TestCase):
    def run_importer(self, args, success=True):
        result = subprocess.run([str(BINARY)] + args, capture_output=True, text=True, timeout=45)
        if success:
            self.assertEqual(result.returncode, 0, result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stderr)
        return result

    def fixture(self, directory, kind="f32", metric="l2"):
        rng = random.Random(812)
        dim, count, queries = 8, 500, 20
        generate = (lambda: rng.randrange(0, 256)) if kind == "u8" else (lambda: rng.uniform(-1, 1))
        vectors = [[generate() for _ in range(dim)] for _ in range(count)]
        query = [[generate() for _ in range(dim)] for _ in range(queries)]
        fmt = "u8bin" if kind == "u8" else "fbin"
        row_format = "<" + ("B" if kind == "u8" else "f") * dim
        # Round F32 inputs before constructing exact GT, matching stored bytes.
        vectors = [struct.unpack(row_format, struct.pack(row_format, *row)) for row in vectors]
        query = [struct.unpack(row_format, struct.pack(row_format, *row)) for row in query]
        directory = Path(directory)
        base, query_path, truth = directory / ("base." + fmt), directory / ("query." + fmt), directory / "gt.ibin"
        for path, rows in [(base, vectors), (query_path, query)]:
            with path.open("wb") as out:
                out.write(struct.pack("<II", len(rows), dim))
                for row in rows:
                    out.write(struct.pack(row_format, *row))
        with truth.open("wb") as out:
            out.write(struct.pack("<II", queries, 10))
            for q in query:
                distance = lambda v: sum((x - y) ** 2 for x, y in zip(q, v)) if metric == "l2" else 1 - sum(x * y for x, y in zip(q, v))
                found = sorted(range(count), key=lambda i: (distance(vectors[i]), i))[:10]
                out.write(struct.pack("<10I", *found))
        args = ["--build-only", "--input", str(base), "--input-format", fmt,
                "--vector-kind", kind, "--metric", metric, "--num-vectors", str(count),
                "--dim", str(dim), "--M", "8", "--ef", "64", "--threads", "2",
                "--seed", "100", "--index-cache", str(directory / "graph.hnsw"),
                "--dataset-sha256", hashlib.sha256(base.read_bytes()).hexdigest(),
                "--keyring", str(directory / "missing.keyring"),
                "--metrics-out", str(directory / "metrics.json"),
                "--progress-out", str(directory / "progress.json")]
        quality = ["--query-input", str(query_path), "--ground-truth", str(truth),
                   "--query-ef", "128", "--query-count", str(queries),
                   "--local-recall-out", str(directory / "quality.json")]
        return args, quality

    def test_build_load_and_local_quality_without_ceph(self):
        for kind, metric in [("f32", "l2"), ("f32", "ip"), ("u8", "l2")]:
            with self.subTest(kind=kind, metric=metric), tempfile.TemporaryDirectory() as folder:
                args, quality = self.fixture(folder, kind, metric)
                self.run_importer(args + ["--index-cache-mode", "build"] + quality)
                first = json.loads((Path(folder) / "quality.json").read_text())
                self.assertGreaterEqual(first["recall_at_k"], 0.98)
                self.assertEqual(first["evaluated_queries"], 20)
                self.assertEqual(first["denominator"], 200)
                metrics = json.loads((Path(folder) / "metrics.json").read_text())
                self.assertEqual(metrics["mode"], "build_only")
                self.assertEqual(metrics["vectors_imported"], 0)
                self.assertEqual(metrics["omap_write_calls"], 0)
                self.assertEqual(json.loads((Path(folder) / "progress.json").read_text())["stage"], "build_only_done")
                self.run_importer(args + ["--index-cache-mode", "load"] + quality)
                self.assertEqual(first, json.loads((Path(folder) / "quality.json").read_text()))
                self.assertTrue(json.loads((Path(folder) / "metrics.json").read_text())["index_cache_loaded"])
                self.run_importer(args + ["--index-cache-mode", "build"], success=False)
                self.run_importer(args + ["--index-cache-mode", "load", "--ef", "65"], success=False)
                self.run_importer(args + ["--index-cache-mode", "load", "--dataset-sha256", "a" * 64], success=False)
                identity = Path(folder) / "graph.hnsw.meta"
                identity.unlink()
                result = self.run_importer(args + ["--index-cache-mode", "load"], success=False)
                self.assertIn("sidecar missing", result.stderr)

    def test_bad_cli_and_short_input_fail_before_cluster_access(self):
        for args in [["--does-not-exist"], ["--threads", "-1"], ["--threads", "4294967296"]]:
            self.run_importer(args, success=False)
        with tempfile.TemporaryDirectory() as folder:
            args, _ = self.fixture(folder)
            self.run_importer(args + ["--import-workers", "6"], success=False)
            base = Path(folder) / "base.fbin"
            with base.open("r+b") as stream:
                stream.truncate(50)
            result = self.run_importer(args + ["--index-cache-mode", "build"], success=False)
            self.assertIn("dataset read failed", result.stderr)


if __name__ == "__main__":
    unittest.main()
