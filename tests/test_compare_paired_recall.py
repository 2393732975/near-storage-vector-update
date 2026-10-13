import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "compare-paired-recall.py"
SPEC = importlib.util.spec_from_file_location("compare_paired_recall", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ComparePairedRecallTest(unittest.TestCase):
    def write_run(self, root, name, rows, recall_k=10, exact=False):
        path = Path(root) / name
        samples = []
        for query_index, hits, denominator in rows:
            sample = {
                "query_index": query_index,
                "update_sequence": query_index,
                "hits": hits,
                "denominator": denominator,
            }
            if exact:
                sample.update(
                    {
                        "target_label": query_index,
                        "inserted_global_id": 1000 + query_index,
                        "inserted_level": query_index % 3,
                        "candidate_hash": 2000 + query_index,
                    }
                )
            samples.append(sample)
        path.write_text(
            json.dumps(
                {
                    "quality": {
                        "recall_k": recall_k,
                        "paired_query_samples": samples,
                    }
                }
            ),
            encoding="utf-8",
        )
        return path

    def test_uses_longest_contiguous_shared_prefix(self):
        with tempfile.TemporaryDirectory() as root:
            compute = self.write_run(
                root, "compute.json", [(0, 5, 10), (1, 6, 10), (2, 9, 10)]
            )
            osd = self.write_run(
                root, "osd.json", [(0, 5, 10), (1, 5, 10), (3, 10, 10)]
            )
            result = MODULE.compare(compute, osd, 0.05)
        self.assertEqual(result["shared_prefix_queries"], 2)
        self.assertAlmostEqual(result["compute_recall"], 0.55)
        self.assertAlmostEqual(result["osd_recall"], 0.50)
        self.assertEqual(result["status"], "pass")

    def test_rejects_regression_beyond_limit(self):
        with tempfile.TemporaryDirectory() as root:
            compute = self.write_run(root, "compute.json", [(0, 8, 10)])
            osd = self.write_run(root, "osd.json", [(0, 7, 10)])
            result = MODULE.compare(compute, osd, 0.005)
        self.assertEqual(result["status"], "fail")

    def test_requires_prefix_from_zero(self):
        with tempfile.TemporaryDirectory() as root:
            compute = self.write_run(root, "compute.json", [(1, 8, 10)])
            osd = self.write_run(root, "osd.json", [(1, 8, 10)])
            with self.assertRaisesRegex(ValueError, "no shared query prefix"):
                MODULE.compare(compute, osd, 0.005)

    def test_exact_fixed_trace_passes_for_identical_samples(self):
        with tempfile.TemporaryDirectory() as root:
            control = self.write_run(
                root, "control.json", [(0, 8, 10), (1, 7, 10)], exact=True
            )
            optimized = self.write_run(
                root, "optimized.json", [(0, 8, 10), (1, 7, 10)], exact=True
            )
            result = MODULE.compare(
                control,
                optimized,
                0.0,
                expected_queries=2,
                require_exact_samples=True,
            )
        self.assertEqual(result["status"], "pass")
        self.assertTrue(result["exact_samples_match"])

    def test_exact_fixed_trace_rejects_candidate_mismatch(self):
        with tempfile.TemporaryDirectory() as root:
            control = self.write_run(
                root, "control.json", [(0, 8, 10), (1, 7, 10)], exact=True
            )
            optimized = self.write_run(
                root, "optimized.json", [(0, 8, 10), (1, 7, 10)], exact=True
            )
            document = json.loads(optimized.read_text(encoding="utf-8"))
            document["quality"]["paired_query_samples"][1]["candidate_hash"] += 1
            optimized.write_text(json.dumps(document), encoding="utf-8")
            result = MODULE.compare(
                control,
                optimized,
                0.0,
                expected_queries=2,
                require_exact_samples=True,
            )
        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["exact_mismatch_queries"], [1])

    def test_fixed_trace_requires_expected_query_count(self):
        with tempfile.TemporaryDirectory() as root:
            control = self.write_run(root, "control.json", [(0, 8, 10)], exact=True)
            optimized = self.write_run(
                root, "optimized.json", [(0, 8, 10)], exact=True
            )
            with self.assertRaisesRegex(ValueError, "fixed-update gate"):
                MODULE.compare(
                    control,
                    optimized,
                    0.0,
                    expected_queries=2,
                    require_exact_samples=True,
                )


if __name__ == "__main__":
    unittest.main()
