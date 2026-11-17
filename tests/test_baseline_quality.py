import importlib.util
import json
from pathlib import Path
import struct
import tempfile
import unittest


SCRIPT = Path(__file__).parents[1] / "scripts" / "baseline-quality.py"
SPEC = importlib.util.spec_from_file_location("baseline_quality", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def write_vectors(path, vectors, kind="f32", declared_count=None):
    dim = len(vectors[0])
    with Path(path).open("wb") as stream:
        stream.write(struct.pack("<II", declared_count or len(vectors), dim))
        for row in vectors:
            stream.write(struct.pack("<" + str(dim) + ("f" if kind == "f32" else "B"), *row))


def write_results(path, labels):
    Path(path).write_text("".join(json.dumps(dict(query_index=i, labels=row)) + "\n"
                                 for i, row in enumerate(labels)))


class BaselineQualityTest(unittest.TestCase):
    def make_dataset(self, root, *, base_vectors=None, query_vectors=None, width=4,
                     kind="f32", metric="l2", trailing_distances=True):
        root = Path(root)
        source = root / "tiny"
        source.mkdir()
        base_vectors = base_vectors or [[x, 0] for x in (0, 1, 2, 4, 8, 16, 32, 64)]
        query_vectors = query_vectors or [[x, 0.1] for x in (0.2, 1.2, 3, 5, 7, 9, 15, 20, 30, 50)]
        write_vectors(source / "base.bin", base_vectors, kind, declared_count=1000000)
        write_vectors(source / "query.bin", query_vectors, kind)
        engine = MODULE.DistanceEngine(metric, len(base_vectors[0]), kind, False)
        rows = [sorted(range(len(base_vectors)), key=lambda i: (engine.distance(q, base_vectors[i]), i))[:width]
                for q in query_vectors]
        MODULE.write_gt(source / "gt.bin", rows)
        if trailing_distances:
            with (source / "gt.bin").open("ab") as stream:
                for q, row in zip(query_vectors, rows):
                    stream.write(struct.pack("<" + str(width) + "f",
                                             *(engine.distance(q, base_vectors[i]) for i in row)))
        cfg = dict(base="base.bin", query="query.bin", gt="gt.bin", count=len(base_vectors),
                   dim=len(base_vectors[0]), kind=kind, metric=metric,
                   points_per_object=3, threads=1)
        manifest = MODULE.prepare_dataset("tiny", root, root / "prepared", seed=27,
                                          config=cfg, split_counts=(2, 4, 4))
        return manifest, base_vectors, query_vectors, rows

    def test_preparation_is_disjoint_reordered_and_handles_larger_base_header(self):
        with tempfile.TemporaryDirectory() as temp:
            manifest, _, _, source_rows = self.make_dataset(temp)
            ids = [set(item["original_query_ids"]) for item in manifest["splits"].values()]
            self.assertTrue(all(not ids[i] & ids[j] for i in range(3) for j in range(i)))
            self.assertEqual(set.union(*ids), set(range(10)))
            self.assertEqual(manifest["base"]["count"], 8)
            self.assertEqual(manifest["base"]["declared_count"], 1000000)
            self.assertTrue(manifest["source_ground_truth"]["has_distance_matrix"])
            for split in manifest["splits"].values():
                self.assertEqual(MODULE.read_gt(split["ground_truth"]),
                                 [source_rows[i] for i in split["original_query_ids"]])
                self.assertEqual(MODULE.sha256(split["vectors"]), split["vectors_sha256"])
            self.assertEqual(MODULE.choose_splits(10, 2, 4, 4, 27),
                             {name: item["original_query_ids"] for name, item in manifest["splits"].items()})

    def test_refuses_insufficient_disjoint_queries(self):
        with self.assertRaisesRegex(ValueError, "not enough"):
            MODULE.choose_splits(1000, 200, 500, 400, 1)

    def test_refuses_vector_truncation_or_wrong_actual_count(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "vectors.bin"
            write_vectors(path, [[1, 2], [3, 4]], declared_count=100)
            with self.assertRaisesRegex(ValueError, "actual vector count mismatch"):
                MODULE.vector_info(path, "f32", 100)
            with path.open("ab") as stream:
                stream.write(b"x")
            with self.assertRaisesRegex(ValueError, "truncated vector row"):
                MODULE.vector_info(path, "f32")

    def test_score_applies_absolute_gate_with_missing_results_in_denominator(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            MODULE.write_gt(root / "truth.bin", [[1, 2], [3, 4]])
            write_results(root / "results.jsonl", [[1, 2], [3]])
            result = MODULE.score_results(root / "results.jsonl", root / "truth.bin", k=2,
                                           min_recall=0.9, bootstrap_samples=20)
            self.assertEqual(result["status"], "fail")
            self.assertEqual(result["recall_at_k"], 0.75)
            self.assertEqual(result["denominator"], 4)
            self.assertEqual(len(result["bootstrap_95_ci"]), 2)

    def test_score_rejects_missing_queries_and_duplicate_labels(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            MODULE.write_gt(root / "truth.bin", [[1, 2], [3, 4]])
            write_results(root / "results.jsonl", [[1, 2]])
            with self.assertRaisesRegex(ValueError, "cover every GT query"):
                MODULE.score_results(root / "results.jsonl", root / "truth.bin", k=2)
            write_results(root / "results.jsonl", [[1, 1], [3, 4]])
            with self.assertRaisesRegex(ValueError, "duplicate result labels"):
                MODULE.score_results(root / "results.jsonl", root / "truth.bin", k=2)

    def test_paired_gate_is_symmetric_and_requires_matching_truth(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            MODULE.write_gt(root / "truth.bin", [[1, 2], [3, 4]])
            write_results(root / "a.jsonl", [[1, 2], [3]])
            write_results(root / "b.jsonl", [[1, 2], [3, 4]])
            a = MODULE.score_results(root / "a.jsonl", root / "truth.bin", k=2,
                                      min_recall=0.7, bootstrap_samples=0)
            b = MODULE.score_results(root / "b.jsonl", root / "truth.bin", k=2,
                                      min_recall=0.7, bootstrap_samples=0,
                                      paired_score=a, max_paired_delta=0.1)
            self.assertEqual(b["status"], "fail")
            self.assertGreater(b["paired_delta"], 0)
            a["ground_truth_sha256"] = "different"
            with self.assertRaisesRegex(ValueError, "identity mismatch"):
                MODULE.score_results(root / "b.jsonl", root / "truth.bin", k=2,
                                       bootstrap_samples=0, paired_score=a)

    def test_dynamic_gt_matches_full_active_enumeration(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manifest, _, _, _ = self.make_dataset(temp)
            trace = root / "trace.jsonl"
            trace.write_text("\n".join(json.dumps(dict(update_sequence=i, target_label=target,
                                                       new_id=8 + i, update_vector_index=i, success=True))
                                       for i, target in enumerate([0, 3])) + "\n")
            metadata = MODULE.dynamic_ground_truth(root / "prepared/split-manifest.json", "validation",
                                                    trace, root / "dynamic.bin", root / "snapshot.json",
                                                    k=2, expected_updates=2, numpy_enabled=False,
                                                    block_rows=3)
            base = MODULE.VectorFile(manifest["base"]["path"], "f32")
            queries = MODULE.VectorFile(manifest["splits"]["validation"]["vectors"], "f32")
            updates = MODULE.VectorFile(manifest["splits"]["update"]["vectors"], "f32")
            try:
                engine = MODULE.DistanceEngine("l2", 2, "f32", False)
                active = {i: base.row(i) for i in range(8)}
                active.update({0: updates.row(0), 3: updates.row(1)})
                expected = [sorted(active, key=lambda label: (engine.distance(queries.row(i), active[label]), label))[:2]
                            for i in range(queries.count)]
                self.assertEqual(MODULE.read_gt(root / "dynamic.bin"), expected)
                self.assertEqual(metadata["active_count"], 8)
                self.assertEqual(metadata["active_replacement_count"], 2)
                self.assertEqual(metadata["ground_truth_sha256"], MODULE.sha256(root / "dynamic.bin"))
            finally:
                base.close()
                queries.close()
                updates.close()

    def test_insufficient_gt_prefix_and_boundary_ties_force_full_scan(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_vectors(root / "base.bin", [[0, 0]] * 6)
            write_vectors(root / "update.bin", [[10, 0]])
            base = MODULE.VectorFile(root / "base.bin", "f32")
            updates = MODULE.VectorFile(root / "update.bin", "f32")
            try:
                for numpy_enabled in (False, True):
                    engine = MODULE.DistanceEngine("l2", 2, "f32", numpy_enabled)
                    neighbors, fallback = MODULE.incremental_active_neighbors(
                        [0, 0], [4, 5], base, {}, updates, engine, 1, 2)
                    self.assertTrue(fallback)
                    self.assertEqual(neighbors, [(0.0, 0)])
                    neighbors, fallback = MODULE.incremental_active_neighbors(
                        [0, 0], [0, 1], base, {0: {"update_vector_index": 0}}, updates, engine, 2, 2)
                    self.assertTrue(fallback)
                    self.assertEqual(neighbors, [(0.0, 1), (0.0, 2)])
            finally:
                base.close()
                updates.close()

    def test_failed_or_incomplete_trace_is_not_certified(self):
        with tempfile.TemporaryDirectory() as temp:
            trace = Path(temp) / "trace.jsonl"
            trace.write_text(json.dumps(dict(update_sequence=1, target_label=0,
                                             update_vector_index=0, success=True)) + "\n")
            with self.assertRaisesRegex(ValueError, "complete prefix"):
                MODULE.read_replacements(trace, 4, 8)
            trace.write_text(json.dumps(dict(update_sequence=0, target_label=0,
                                             update_vector_index=0, success=False)) + "\n")
            with self.assertRaisesRegex(ValueError, "failed update"):
                MODULE.read_replacements(trace, 4, 8)

    def test_repeated_label_requires_explicit_commit_order(self):
        with tempfile.TemporaryDirectory() as temp:
            trace = Path(temp) / "trace.jsonl"
            rows = [dict(update_sequence=i, target_label=0, update_vector_index=i, success=True)
                    for i in range(2)]
            trace.write_text("\n".join(json.dumps(row) for row in rows))
            with self.assertRaisesRegex(ValueError, "commit_sequence"):
                MODULE.read_replacements(trace, 4, 8)
            rows[0]["commit_sequence"], rows[1]["commit_sequence"] = 20, 10
            trace.write_text("\n".join(json.dumps(row) for row in rows))
            self.assertEqual(MODULE.read_replacements(trace, 4, 8)[0]["update_vector_index"], 0)

    def test_full_scan_verification_passes_and_detects_wrong_gt(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manifest, _, _, _ = self.make_dataset(temp)
            path = root / "prepared/split-manifest.json"
            result = MODULE.verify_base_ground_truth(path, "calibration", sample_queries=2,
                                                      k=2, numpy_enabled=False, block_rows=3)
            self.assertEqual(result["status"], "pass")
            truth_path = Path(manifest["splits"]["calibration"]["ground_truth"])
            bad = MODULE.read_gt(truth_path)
            bad[0][:2] = [label for label in range(8) if label not in bad[0]][:2]
            MODULE.write_gt(truth_path, bad)
            manifest["splits"]["calibration"]["ground_truth_sha256"] = MODULE.sha256(truth_path)
            MODULE.write_json(path, manifest)
            result = MODULE.verify_base_ground_truth(path, "calibration", sample_queries=2,
                                                      k=2, numpy_enabled=False, block_rows=3)
            self.assertEqual(result["status"], "fail")

    def test_distance_backends_match_sequential_float32_for_l2_and_ip(self):
        vectors = [[0.1, -0.2, 1e3], [0.2, -0.4, 3e2], [0.4, 0.5, -20]]
        query = [0.15, 0.25, 123]
        for metric in ("l2", "ip"):
            scalar = MODULE.DistanceEngine(metric, 3, "f32", False)
            optional_numpy = MODULE.DistanceEngine(metric, 3, "f32", True)
            self.assertEqual(list(optional_numpy.distances(query, vectors)),
                             list(scalar.distances(query, vectors)))

    def test_gt_verification_accepts_alternative_ids_at_equal_distance(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manifest, _, _, _ = self.make_dataset(
                temp, base_vectors=[[0, 0]] * 8, query_vectors=[[0, 0]] * 10)
            truth_path = Path(manifest["splits"]["calibration"]["ground_truth"])
            MODULE.write_gt(truth_path, [[4, 5, 6, 7]] * 2)
            manifest["splits"]["calibration"]["ground_truth_sha256"] = MODULE.sha256(truth_path)
            path = root / "prepared/split-manifest.json"
            MODULE.write_json(path, manifest)
            for numpy_enabled in (False, True):
                result = MODULE.verify_base_ground_truth(path, "calibration", sample_queries=2,
                                                          k=2, numpy_enabled=numpy_enabled, block_rows=3)
                self.assertEqual(result["status"], "pass")
                self.assertFalse(result["samples"][0]["strict_label_set_match"])

    def test_u8_distance_and_inner_product_full_scan_match_scalar_enumeration(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for kind, metric, vectors in [
                    ("u8", "l2", [[0, 0], [255, 255], [255, 0], [1, 1]]),
                    ("f32", "ip", [[0.1, 0.2], [2, 1], [-1, 0.2], [10, 1]])]:
                path = root / (kind + ".bin")
                write_vectors(path, vectors, kind)
                base = MODULE.VectorFile(path, kind)
                query = base.row(0)
                scalar = MODULE.DistanceEngine(metric, 2, kind, False)
                try:
                    expected = sorted((scalar.distance(query, base.row(i)), i)
                                      for i in range(len(vectors)))[:2]
                    for numpy_enabled in (False, True):
                        engine = MODULE.DistanceEngine(metric, 2, kind, numpy_enabled)
                        actual = MODULE.exact_active_neighbors(query, base, {}, None, engine, 2, 2)
                        self.assertEqual(actual, expected)
                finally:
                    base.close()


if __name__ == "__main__":
    unittest.main()
