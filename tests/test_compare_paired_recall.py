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
    def write_run(self, root, name, rows, recall_k=10):
        path = Path(root) / name
        path.write_text(
            json.dumps(
                {
                    "quality": {
                        "recall_k": recall_k,
                        "paired_query_samples": [
                            {
                                "query_index": query_index,
                                "update_sequence": query_index,
                                "hits": hits,
                                "denominator": denominator,
                            }
                            for query_index, hits, denominator in rows
                        ],
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


if __name__ == "__main__":
    unittest.main()
