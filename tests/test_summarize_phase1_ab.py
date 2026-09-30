import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "summarize-phase1-ab.py"
SPEC = importlib.util.spec_from_file_location("summarize_phase1_ab", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class Phase1SummaryTest(unittest.TestCase):
    def write_run(self, root, repetition, mode, latency, throughput, recall):
        output = root / f"rep-{repetition}" / mode / "gist1m"
        output.mkdir(parents=True)
        update = {
            "distance_mode": mode,
            "storage_access_mode": "raw_rados" if mode == "compute" else "cls",
            "vectors_processed": 100,
            "failed_updates": 0,
            "throughput_updates_per_sec": throughput,
            "avg_update_latency_ms": latency,
            "p99_update_latency_ms": latency * 2,
            "remote_patch_calls": 500,
            "quality": {"precommit_static_recall_at_k": recall},
            "observability": {
                "update_attempts": 100,
                "total_cls_exec_calls": 0 if mode == "compute" else 1000,
                "total_raw_rados_calls": 1200 if mode == "compute" else 0,
                "distance_batches": 600,
                "candidates_per_distance_batch": 1.25,
                "logical_distance_query_bytes": 102400,
                "cls_request_bytes": 0 if mode == "compute" else 204800,
                "cls_reply_bytes": 0 if mode == "compute" else 307200,
                "raw_rados_request_bytes": 102400 if mode == "compute" else 0,
                "raw_rados_reply_bytes": 409600 if mode == "compute" else 0,
            },
        }
        check = {"status": "pass", "errors": 0, "warnings": 0}
        (output / "update.json").write_text(json.dumps(update), encoding="utf-8")
        (output / "index-check.json").write_text(json.dumps(check), encoding="utf-8")

    def test_aggregate_and_markdown(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.write_run(root, 1, "compute", 100.0, 40.0, 0.80)
            self.write_run(root, 1, "osd", 80.0, 50.0, 0.79)
            self.write_run(root, 2, "compute", 120.0, 35.0, 0.82)
            self.write_run(root, 2, "osd", 100.0, 45.0, 0.81)

            runs = MODULE.load_runs(root)
            aggregate = MODULE.aggregate(runs)
            self.assertEqual(len(runs), 4)
            self.assertEqual(aggregate["gist1m"]["compute"]["valid_runs"], 2)
            self.assertAlmostEqual(
                aggregate["gist1m"]["compute"]["avg_latency_ms"]["mean"], 110.0
            )
            self.assertAlmostEqual(
                aggregate["gist1m"]["osd"]["cls_kib_per_update"]["mean"], 5.0
            )
            self.assertAlmostEqual(
                aggregate["gist1m"]["compute"]["raw_rados_calls_per_update"]["mean"],
                12.0,
            )
            self.assertAlmostEqual(
                aggregate["gist1m"]["compute"]["raw_rados_kib_per_update"]["mean"],
                5.0,
            )

            report = root / "summary.md"
            MODULE.write_markdown(root, aggregate, report)
            text = report.read_text(encoding="utf-8")
            self.assertIn("阶段 1 Compute/OSD 严格 A/B 汇总", text)
            self.assertIn("fresh-pool-after-import", text)
            self.assertIn("| gist1m | compute | raw_rados | 12.00 | 0.00", text)

    def test_osd_only_markdown(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.write_run(root, 1, "osd", 80.0, 50.0, 0.79)

            aggregate = MODULE.aggregate(MODULE.load_runs(root))
            report = root / "summary.md"
            MODULE.write_markdown(root, aggregate, report)
            text = report.read_text(encoding="utf-8")
            self.assertIn("OSD/CLS 单路径正式复测汇总", text)
            self.assertNotIn("## OSD 相对 Compute", text)


if __name__ == "__main__":
    unittest.main()
