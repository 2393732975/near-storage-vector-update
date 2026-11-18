import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import struct
import subprocess
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock

PATH = Path(__file__).parents[1] / "scripts/run-high-recall-preparation.py"
SPEC = importlib.util.spec_from_file_location("preparation", PATH)
MOD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MOD)


class PreparationTests(unittest.TestCase):
    def test_retained_warning_requires_event_guard_and_scoped_pool_identities(self):
        with tempfile.TemporaryDirectory() as folder:
            suite, doc = self.gate(folder)
            doc["health"]["checks"] = dict(BLUESTORE_SLOW_OP_ALERT={}, POOL_NO_REDUNDANCY={})
            suite.resume_root = Path("/prior-run")
            details = dict(checks=dict(POOL_NO_REDUNDANCY=dict(detail=[
                dict(message="pool 'nsvu_owner_%d' has no replicas configured" % i) for i in range(5)])))
            original = suite.ceph_json
            suite.ceph_json = lambda *cmd: copy.deepcopy(details) if cmd == ("health", "detail") else original(*cmd)
            with mock.patch.object(MOD.subprocess, "check_output", return_value=MOD.FSID), \
                    self.assertRaisesRegex(RuntimeError, "unexpected initial health"):
                suite.cluster_gate(initial=True)
            suite.slow_guard = mock.Mock()
            suite.slow_guard.check.return_value = details
            with mock.patch.object(MOD.subprocess, "check_output", return_value=MOD.FSID):
                suite.cluster_gate(initial=True)
            suite.slow_guard.check.assert_called_once()
            details["checks"]["POOL_NO_REDUNDANCY"]["detail"].append(dict(message="pool 'rbd' has no replicas"))
            with mock.patch.object(MOD.subprocess, "check_output", return_value=MOD.FSID), \
                    self.assertRaisesRegex(RuntimeError, "outside the six"):
                suite.cluster_gate(initial=True)

    def test_existing_health_name_does_not_bypass_a_fresh_slow_event(self):
        with tempfile.TemporaryDirectory() as folder:
            suite, _ = self.gate(folder)
            suite.slow_guard = mock.Mock()
            suite.slow_guard.check.side_effect = RuntimeError("new slow event")
            suite.allowed_health.add("BLUESTORE_SLOW_OP_ALERT")
            with mock.patch.object(MOD.subprocess, "check_output", return_value=MOD.FSID), \
                    self.assertRaisesRegex(RuntimeError, "new slow event"):
                suite.cluster_gate()

    def resume_suite(self, root, prior=None, offline=False):
        root.mkdir()
        suite = object.__new__(MOD.Suite)
        suite.root, suite.repo = root, PATH.parents[1]
        suite.args = SimpleNamespace(max_hours=36, min_free_gib=1, max_artifact_gib=1,
                                     datasets=["gist1m"], keyring=Path("/not/read"))
        suite.started, suite.current, suite.steps = time.monotonic(), None, []
        suite.resume_root, suite.offline_only = prior, offline
        suite.reused_splits, suite.reused_graphs, suite.reused_cases = {}, {}, {}
        suite.reference_roots, suite.reference_bytes, suite.resume_evidence = [], 0, {}
        suite.bin = root / "bin"
        suite.bin.mkdir()
        for name in ("nsvu-base-importer", "nsvu-index-checker", "libcls_hnsw_global.so",
                     "baseline-quality.py", "index-content-digest.py"):
            (suite.bin / name).write_bytes(name.encode())
        return suite

    def fake_prior(self, root, datasets=("gist1m",)):
        root.mkdir()
        source = root / "source"
        source.mkdir()
        for filename, values in (("base.fbin", list(range(32))),
                                 ("query.fbin", [.1, 1.1, 2.1, 3.1, 4.1, 5.1])):
            (source / filename).write_bytes(struct.pack("<II", len(values), 1) +
                                            struct.pack("<%df" % len(values), *values))
        gt = [sorted(range(32), key=lambda x: (abs(x - q), x))[:16]
              for q in (.1, 1.1, 2.1, 3.1, 4.1, 5.1)]
        (source / "gt.ibin").write_bytes(struct.pack("<II", 6, 16) +
                                         b"".join(struct.pack("<16I", *x) for x in gt))
        base_info = dict(path=str(source / "base.fbin"), count=32, declared_count=32,
                         dim=1, kind="f32", format="fbin")
        query_info = dict(path=str(source / "query.fbin"), count=6, declared_count=6,
                          dim=1, kind="f32", format="fbin")
        truth_info = dict(path=str(source / "gt.ibin"), count=6, width=16)
        for info in (base_info, query_info, truth_info):
            p = Path(info["path"])
            info.update(size_bytes=p.stat().st_size, mtime_ns=p.stat().st_mtime_ns,
                        sha256=MOD.sha(p))
        bins = {name: hashlib.sha256(name.encode()).hexdigest() for name in
                ("nsvu-base-importer", "nsvu-index-checker", "libcls_hnsw_global.so",
                 "baseline-quality.py", "index-content-digest.py")}
        MOD.write_json(root / "manifest.json", dict(expected_fsid=MOD.FSID, binaries=bins))
        MOD.write_json(root / "status.json", dict(state="stopped"))
        for dataset in datasets:
            splits = {}
            folder = root / "splits" / dataset
            folder.mkdir(parents=True)
            for name, ids in (("calibration", [0, 1]), ("validation", [2, 3]), ("update", [4, 5])):
                vectors, truth = folder / (name + ".fbin"), folder / (name + ".gt.ibin")
                original = (source / "query.fbin").read_bytes()
                vectors.write_bytes(struct.pack("<II", 2, 1) +
                                    b"".join(original[8 + i * 4:12 + i * 4] for i in ids))
                truth.write_bytes(struct.pack("<II", 2, 16) +
                                  b"".join(struct.pack("<16I", *gt[i]) for i in ids))
                splits[name] = dict(count=2, original_query_ids=ids, vectors=str(vectors),
                                    ground_truth=str(truth), vectors_sha256=MOD.sha(vectors),
                                    ground_truth_sha256=MOD.sha(truth))
            cfg = dict(count=32, dim=1, kind="f32", metric="l2", threads=1, points_per_object=4)
            manifest = dict(dataset=dataset, schema_version=1, base=base_info, config=cfg,
                            source_queries=query_info, source_ground_truth=truth_info, splits=splits)
            MOD.write_json(folder / "split-manifest.json", manifest)
            MOD.write_json(folder / "gt-verification.json", dict(
                status="pass", dataset=dataset, base_sha256=base_info["sha256"],
                query_vectors_sha256=splits["calibration"]["vectors_sha256"],
                source_ground_truth_sha256=splits["calibration"]["ground_truth_sha256"],
                recall_k=10, sample_queries=2, samples=[dict(status="pass"), dict(status="pass")]))
            graph = root / "graphs" / dataset / "m16-efc200"
            graph.mkdir(parents=True)
            cache = graph / "base.hnsw"
            cache.write_bytes(b"small stand-in for an independently tested HNSW cache")
            (graph / "base.hnsw.meta").write_text(
                "format=nsvu-hnsw-cache-v1\nM=16\nef_construction=200\nnum_vectors=32\n"
                "dim=1\nvector_kind=2\nmetric=1\nthreads=1\nseed=100\ndataset_sha256=" +
                base_info["sha256"] + "\n")
            quality = dict(evaluation="local_hnsw_screen_only", query_ef=256, recall_k=10,
                           evaluated_queries=2, denominator=20, hits=20, recall_at_k=1.0,
                           queries=[dict(query_index=i, hits=10, denominator=10) for i in range(2)])
            for name in ("calibration-ef256.json", "validation.json"):
                MOD.write_json(graph / name, quality)
            MOD.write_json(graph.parent / "selected.json", dict(
                M=16, ef_construction=200, ef_query=256, ef_update=256,
                cache=str(cache), cache_sha256=MOD.sha(cache), local_quality_only=True,
                remote_and_dynamic_quality="not_yet_certified"))
        if "gist1m" in datasets:
            trial_root = root / "import-trials/gist1m"
            objects = [dict(pool="nsvu_meta", object="meta", bytes=0,
                            payload_sha256=hashlib.sha256(b"").hexdigest(), omap_entries=10)]
            digest = hashlib.sha256(json.dumps(objects, sort_keys=True,
                                              separators=(",", ":")).encode()).hexdigest()
            summary = []
            for rep in (1, 2, 3):
                for workers, batch in ((1, 2048), (2, 2048), (5, 2048), (5, 8192)):
                    trial = trial_root / ("rep-%d-w%d-b%d" % (rep, workers, batch))
                    metrics = dict(M=16, ef_construction=200, num_vectors=32, vectors_imported=32,
                                   dim=1, points_per_object=4, owners=5, import_workers=workers,
                                   omap_batch=batch, total_seconds=10 - workers - int(batch == 8192))
                    MOD.write_json(trial / "import.json", metrics)
                    MOD.write_json(trial / "checker.json", dict(status="pass", errors=0, warnings=0,
                                   nodes_checked=32, nodes_expected=32, label_mappings=32))
                    MOD.write_json(trial / "content-digest.json", dict(
                        fsid=MOD.FSID, objects=objects, logical_sha256=digest))
                    summary.append(dict(workers=workers, omap_batch=batch, repetition=rep,
                                        metrics=metrics, path=str(trial)))
            MOD.write_json(trial_root / "summary.json", summary)
            MOD.write_json(trial_root / "selected-settings.json", dict(workers=5, omap_batch=8192))
        return root

    def gate(self, directory, status=None, nodes=None):
        suite = object.__new__(MOD.Suite)
        suite.args = mock.Mock(keyring=Path("/not/read"))
        suite.root = Path(directory)
        suite.allowed_health = {"MGR_MODULE_ERROR", "POOL_NO_REDUNDANCY", "LARGE_OMAP_OBJECTS"}
        document = status if status is not None else dict(
            osdmap=dict(num_osds=5, num_up_osds=5, num_in_osds=5),
            pgmap=dict(num_pgs=39, pgs_by_state=[dict(state_name="active+clean", count=39)]),
            health=dict(checks={}))
        capacities = nodes if nodes is not None else [dict(id=i, utilization=5) for i in range(5)]
        suite.ceph_json = lambda *cmd: copy.deepcopy(document if cmd == ("status",)
                                                    else dict(nodes=capacities))
        return suite, document

    def test_reset_confirmation_is_required_before_any_setup(self):
        result = subprocess.run(["python3", str(PATH), "--run-root", "/must-not-create"],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("--confirm-reset is required", result.stderr)

    def test_missing_or_inconsistent_pg_evidence_fails_closed(self):
        with tempfile.TemporaryDirectory() as folder:
            for pgmap in [dict(num_pgs=39, pgs_by_state=[]),
                          dict(num_pgs=39, pgs_by_state=[dict(state_name="active+clean", count=38)]),
                          dict(num_pgs=39, pgs_by_state=[dict(state_name="active+degraded", count=39)])]:
                suite, doc = self.gate(folder)
                doc["pgmap"] = pgmap
                with mock.patch.object(MOD.subprocess, "check_output", return_value=MOD.FSID), \
                        self.assertRaisesRegex(RuntimeError, "PGs"):
                    suite.cluster_gate()

    def test_capacity_and_manager_error_changes_fail_closed(self):
        with tempfile.TemporaryDirectory() as folder:
            suite, _ = self.gate(folder, nodes=[])
            with mock.patch.object(MOD.subprocess, "check_output", return_value=MOD.FSID), \
                    self.assertRaisesRegex(RuntimeError, "capacity report"):
                suite.cluster_gate()
            suite, doc = self.gate(folder)
            doc["health"]["checks"]["MGR_MODULE_ERROR"] = dict(
                summary=dict(message="another module has failed"))
            with mock.patch.object(MOD.subprocess, "check_output", return_value=MOD.FSID), \
                    self.assertRaisesRegex(RuntimeError, "manager module"):
                suite.cluster_gate()

    def test_wrong_fsid_is_rejected_before_status_query(self):
        with tempfile.TemporaryDirectory() as folder:
            suite, _ = self.gate(folder)
            suite.ceph_json = mock.Mock(side_effect=AssertionError("must not query other cluster"))
            with mock.patch.object(MOD.subprocess, "check_output", return_value="wrong"), \
                    self.assertRaisesRegex(RuntimeError, "FSID"):
                suite.cluster_gate()

    def test_resume_reuses_verified_graph_and_all_complete_gist_trials(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            prior = self.fake_prior(root / "old")
            suite = self.resume_suite(root / "new", prior)
            suite.load_resume()
            _, manifest = suite.prepare("gist1m")
            selected = suite.qualify("gist1m", manifest)
            suite.import_case = mock.Mock(side_effect=AssertionError("complete trials must not rerun"))
            suite.import_trials("gist1m", manifest, selected)
            self.assertEqual(Path(selected["cache"]).parent.parent.parent, prior / "graphs")
            self.assertEqual(len(MOD.read_json(suite.root / "import-trials/gist1m/summary.json")), 12)
            self.assertFalse((suite.root / "graphs/gist1m/m16-efc200/base.hnsw").exists())
            self.assertGreater(suite.reference_bytes, 0)
            self.assertTrue(MOD.read_json(suite.root / "resume-manifest.json")["local_quality_only"])

    def test_offline_completed_resume_can_be_resumed_again(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            prior = self.fake_prior(root / "old")
            first = self.resume_suite(root / "middle", prior, offline=True)
            first.load_resume()
            MOD.write_json(first.root / "manifest.json", MOD.read_json(prior / "manifest.json"))
            first.status("offline_qualification_complete")
            second = self.resume_suite(root / "new", first.root)
            second.load_resume()
            self.assertEqual(set(second.reference_roots), {prior, first.root})
            self.assertEqual(len(second.reused_cases["gist1m"]), 12)
            self.assertEqual(second.reused_graphs["gist1m"]["cache"],
                             first.reused_graphs["gist1m"]["cache"])

    def test_changed_cache_or_metadata_refuses_reuse(self):
        for kind in ("cache", "metadata", "query", "quality"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                prior = self.fake_prior(root / "old")
                graph = prior / "graphs/gist1m/m16-efc200"
                if kind == "cache":
                    (graph / "base.hnsw").write_bytes(b"changed")
                elif kind == "metadata":
                    p = graph / "base.hnsw.meta"
                    p.write_text(p.read_text().replace("M=16", "M=32"))
                elif kind == "query":
                    (prior / "splits/gist1m/validation.fbin").write_bytes(b"changed")
                else:
                    p = graph / "validation.json"
                    doc = MOD.read_json(p)
                    doc["queries"].pop()
                    MOD.write_json(p, doc)
                suite = self.resume_suite(root / "new", prior)
                with self.assertRaises(RuntimeError):
                    suite.load_resume()

    def test_incomplete_case_is_reimported_in_new_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            prior = self.fake_prior(root / "old")
            missing = prior / "import-trials/gist1m/rep-1-w1-b2048/checker.json"
            missing.unlink()
            suite = self.resume_suite(root / "new", prior)
            suite.load_resume()
            self.assertEqual(len(suite.reused_cases["gist1m"]), 11)
            self.assertFalse((suite.root / "import-trials/gist1m/selected-settings.json").exists())
            def fresh(*args, **kwargs):
                target = suite.root / "fresh-trial"
                shutil.copytree(missing.parent, target)
                shutil.copy2(prior / "import-trials/gist1m/rep-2-w1-b2048/checker.json", target / "checker.json")
                return target
            suite.import_case = mock.Mock(side_effect=fresh)
            suite.import_trials("gist1m", suite.reused_splits["gist1m"], suite.reused_graphs["gist1m"])
            suite.import_case.assert_called_once()
            self.assertFalse(missing.exists())
            rows = MOD.read_json(suite.root / "import-trials/gist1m/summary.json")
            self.assertEqual(len(rows), 12)
            self.assertEqual(sum(Path(x["path"]) == suite.root / "fresh-trial" for x in rows), 1)

    def test_changed_reimport_digest_cannot_be_hidden_by_later_reused_cases(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            prior = self.fake_prior(root / "old")
            (prior / "import-trials/gist1m/rep-1-w1-b2048/checker.json").unlink()
            suite = self.resume_suite(root / "new", prior)
            suite.load_resume()
            source = prior / "import-trials/gist1m/rep-1-w1-b2048"
            target = suite.root / "bad-import"
            shutil.copytree(source, target)
            doc = MOD.read_json(target / "content-digest.json")
            doc["logical_sha256"] = "a" * 64
            MOD.write_json(target / "content-digest.json", doc)
            suite.import_case = mock.Mock(return_value=target)
            with self.assertRaisesRegex(RuntimeError, "changed persisted logical content"):
                suite.import_trials("gist1m", suite.reused_splits["gist1m"], suite.reused_graphs["gist1m"])

    def test_partial_deep_import_directory_is_never_continued_in_place(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            prior = self.fake_prior(root / "old", ("gist1m", "deep100m"))
            partial = prior / "import-trials/deep100m/rep-1-w1-b2048/progress.json"
            MOD.write_json(partial, dict(stage="write_vector_payloads", processed=17, total=32))
            original = partial.read_bytes()
            suite = self.resume_suite(root / "new", prior)
            suite.load_resume()
            self.assertFalse(suite.reused_cases.get("deep100m"))
            def fresh(dataset, manifest, selected, workers, batch, repetition, digest):
                folder = suite.root / ("new-w%d-b%d" % (workers, batch))
                MOD.write_json(folder / "import.json", dict(total_seconds=10 - workers))
                return folder
            suite.import_case = mock.Mock(side_effect=fresh)
            suite.import_trials("deep100m", suite.reused_splits["deep100m"], suite.reused_graphs["deep100m"])
            self.assertEqual(suite.import_case.call_count, 2)
            self.assertEqual(partial.read_bytes(), original)

    def test_online_execute_records_files_after_guard_import_creates_bytecode_cache(self):
        with tempfile.TemporaryDirectory() as temp:
            suite = self.resume_suite(Path(temp) / "new")
            suite.args.datasets = ["sift100m"]
            helper = suite.bin / "baseline_cluster_guard.py"
            helper.write_text("GUARD_LOADED = True\n")
            expected = {p.name: MOD.sha(p) for p in suite.bin.iterdir()}

            def initial_gate(initial=False):
                if initial:
                    spec = importlib.util.spec_from_file_location("fixture_guard", helper)
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    self.assertTrue(module.GUARD_LOADED)

            suite.cluster_gate = mock.Mock(side_effect=initial_gate)
            suite.run = mock.Mock()
            suite.prepare = mock.Mock(return_value=(Path("unused"), {}))
            suite.qualify = mock.Mock(return_value={})
            suite.import_trials = mock.Mock()
            suite.execute()
            self.assertTrue((suite.bin / "__pycache__").is_dir())
            self.assertEqual(MOD.read_json(suite.root / "manifest.json")["binaries"], expected)
            self.assertEqual(MOD.read_json(suite.root / "status.json")["state"], "first_batch_complete")

    def test_offline_execute_never_queries_or_cleans_cluster(self):
        with tempfile.TemporaryDirectory() as temp:
            suite = self.resume_suite(Path(temp) / "new", offline=True)
            suite.args.datasets = ["sift100m"]
            suite.cluster_gate = mock.Mock(side_effect=AssertionError("offline must not contact Ceph"))
            suite.run = mock.Mock(side_effect=AssertionError("mocked graph work requires no commands"))
            suite.prepare = mock.Mock(return_value=(Path("unused"), {}))
            suite.qualify = mock.Mock(return_value={})
            suite.execute()
            suite.cluster_gate.assert_not_called()
            suite.run.assert_not_called()
            state = MOD.read_json(suite.root / "status.json")
            self.assertEqual(state["state"], "offline_qualification_complete")
            self.assertTrue(state["experiment_pools_untouched"])
