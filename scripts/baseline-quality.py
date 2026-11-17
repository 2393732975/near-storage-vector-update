#!/usr/bin/env python3
"""Prepare isolated queries and score active-label Recall for baseline experiments.

No command connects to Ceph. Dynamic truth is exact conditional on the supplied
base top-L truth: retain its surviving base candidates, add every current
replacement, and scan the full base if the prefix is insufficient or its boundary
could affect a top-K tie. Float distances use sequential float32 accumulation.
NumPy, when available, accelerates independent vectors without changing reduction
order; a slower standard-library implementation remains available.
"""

import argparse
import bisect
import hashlib
import heapq
import json
import math
from pathlib import Path
import random
import struct
import sys
from functools import lru_cache


DATASETS = {
    "gist1m": dict(base="base.1M.fbin", query="query.public.1K.fbin",
                   gt="gt.public.1K.top1000.ibin", count=1_000_000, dim=960,
                   kind="f32", metric="l2", points_per_object=16000, threads=32),
    "text2image10m": dict(base="base.10M.fbin", query="query.public.100K.fbin",
                         gt="text2image-10M.gt.bin", count=10_000_000, dim=200,
                         kind="f32", metric="ip", points_per_object=60000, threads=48),
    "deep100m": dict(base="base.100M.fbin", query="query.public.10K.fbin",
                     gt="deep-100M.gt.bin", count=100_000_000, dim=96,
                     kind="f32", metric="l2", points_per_object=160000, threads=64),
    "sift100m": dict(base="base.u8bin", query="queries.u8bin", gt="gt_100.bin",
                     count=100_000_000, dim=128, kind="u8", metric="l2",
                     points_per_object=250000, threads=64),
}


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n",
                         encoding="utf-8")
    temporary.replace(path)


def header(path):
    with Path(path).open("rb") as stream:
        raw = stream.read(8)
    if len(raw) != 8:
        raise ValueError(f"truncated binary header: {path}")
    count, width = struct.unpack("<II", raw)
    if count <= 0 or width <= 0:
        raise ValueError(f"invalid binary dimensions: {path}")
    return count, width


def vector_info(path, kind, expected_count=None, expected_dim=None):
    path = Path(path).resolve()
    declared_count, dim = header(path)
    elem = 4 if kind == "f32" else 1
    if kind not in ("f32", "u8"):
        raise ValueError("vector kind must be f32 or u8")
    size = path.stat().st_size
    row_bytes = dim * elem
    if (size - 8) % row_bytes:
        raise ValueError(f"truncated vector row: {path}")
    actual_count = (size - 8) // row_bytes
    if actual_count <= 0 or declared_count < actual_count:
        raise ValueError(f"invalid vector count/size: {path}")
    if expected_count is not None and actual_count != expected_count:
        raise ValueError(f"actual vector count mismatch: {path}: {actual_count} != {expected_count}")
    if expected_dim is not None and dim != expected_dim:
        raise ValueError(f"vector dimension mismatch: {path}")
    return dict(path=str(path), count=actual_count, declared_count=declared_count,
                dim=dim, kind=kind, format="fbin" if kind == "f32" else "u8bin",
                size_bytes=size, mtime_ns=path.stat().st_mtime_ns)


def gt_info(path):
    path = Path(path).resolve()
    count, width = header(path)
    matrix_bytes = count * width * 4
    size = path.stat().st_size
    if size not in (8 + matrix_bytes, 8 + 2 * matrix_bytes):
        raise ValueError(f"GT must contain an ID matrix and optional distance matrix: {path}")
    return dict(path=str(path), count=count, width=width,
                has_distance_matrix=size == 8 + 2 * matrix_bytes, size_bytes=size)


def read_gt(path):
    info = gt_info(path)
    rows = []
    with Path(path).open("rb") as stream:
        stream.seek(8)
        for _ in range(info["count"]):
            raw = stream.read(info["width"] * 4)
            rows.append(list(struct.unpack("<" + str(info["width"]) + "I", raw)))
    return rows


def write_gt(path, rows):
    if not rows or not rows[0] or any(len(row) != len(rows[0]) for row in rows):
        raise ValueError("GT rows must be nonempty and rectangular")
    with Path(path).open("wb") as stream:
        stream.write(struct.pack("<II", len(rows), len(rows[0])))
        for row in rows:
            if any(not isinstance(x, int) or x < 0 or x >= 2**32 for x in row):
                raise ValueError("GT label must fit uint32")
            stream.write(struct.pack("<" + str(len(row)) + "I", *row))


def choose_splits(query_count, calibration_count, validation_count, update_count, seed):
    if min(calibration_count, validation_count, update_count) <= 0:
        raise ValueError("split counts must be positive")
    if calibration_count + validation_count + update_count > query_count:
        raise ValueError("not enough queries for disjoint calibration/validation/update splits")
    ids = list(range(query_count))
    random.Random(seed).shuffle(ids)
    a = calibration_count
    b = a + validation_count
    return {"calibration": ids[:a], "validation": ids[a:b], "update": ids[b:b + update_count]}


def prepare_dataset(dataset, dataset_root, output_dir, seed=20261117, config=None,
                    split_counts=None):
    cfg = dict(config or DATASETS[dataset])
    source_root = Path(dataset_root) / dataset
    base = vector_info(source_root / cfg["base"], cfg["kind"], cfg["count"], cfg["dim"])
    query = vector_info(source_root / cfg["query"], cfg["kind"], expected_dim=cfg["dim"])
    gt = gt_info(source_root / cfg["gt"])
    if gt["count"] != query["count"] or gt["width"] < 10 and cfg["count"] >= 10:
        raise ValueError("query/GT count mismatch or GT width below Recall@10")
    counts = split_counts or ((200, 400, 400) if dataset == "gist1m" else (1000, 1000, 2000))
    splits = choose_splits(query["count"], *counts, seed=seed)
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise ValueError(f"refusing to overwrite nonempty split directory: {output}")
    for source in (base, query, gt):
        source["sha256"] = sha256(source["path"])
    manifest = dict(schema_version=1, dataset=dataset, seed=seed,
                    config={k: v for k, v in cfg.items() if k not in ("base", "query", "gt")},
                    base=base, source_queries=query, source_ground_truth=gt,
                    note="Disjoint query identities; original base GT requires source/metric verification.",
                    splits={})
    row_bytes = cfg["dim"] * (4 if cfg["kind"] == "f32" else 1)
    with Path(query["path"]).open("rb") as queries, Path(gt["path"]).open("rb") as truths:
        for split, ids in splits.items():
            vectors_path = output / (split + "." + query["format"])
            truth_path = output / (split + ".gt.ibin")
            rows = []
            with vectors_path.open("wb") as vectors:
                vectors.write(struct.pack("<II", len(ids), cfg["dim"]))
                for query_id in ids:
                    queries.seek(8 + query_id * row_bytes)
                    data = queries.read(row_bytes)
                    if len(data) != row_bytes:
                        raise ValueError("query row unexpectedly truncated")
                    vectors.write(data)
                    truths.seek(8 + query_id * gt["width"] * 4)
                    row = list(struct.unpack("<" + str(gt["width"]) + "I",
                                             truths.read(gt["width"] * 4)))
                    if len(set(row)) != len(row) or any(label >= cfg["count"] for label in row):
                        raise ValueError(f"invalid/duplicate/out-of-range GT IDs for source query {query_id}")
                    rows.append(row)
            write_gt(truth_path, rows)
            manifest["splits"][split] = dict(count=len(ids), original_query_ids=ids,
                                             vectors=str(vectors_path), ground_truth=str(truth_path),
                                             vectors_sha256=sha256(vectors_path),
                                             ground_truth_sha256=sha256(truth_path))
    manifest["split_identity_sha256"] = canonical_hash(
        {name: item["original_query_ids"] for name, item in manifest["splits"].items()})
    write_json(output / "split-manifest.json", manifest)
    return manifest


def load_results(path):
    text = Path(path).read_text(encoding="utf-8")
    try:
        document = json.loads(text)
    except json.JSONDecodeError:
        document = [json.loads(line) for line in text.splitlines() if line.strip()]
    if isinstance(document, dict):
        document = document.get("queries", document.get("results", [document]))
    if not isinstance(document, list):
        raise ValueError("query results must be JSONL or a query list")
    result = {}
    for row in document:
        index = row.get("query_index")
        if not isinstance(index, int) or isinstance(index, bool) or index < 0 or index in result:
            raise ValueError("invalid or duplicate query_index")
        if "labels" in row:
            labels = row["labels"]
        else:
            labels = [item["label"] for item in row.get("results", [])]
        if not isinstance(labels, list) or any(not isinstance(label, int) or isinstance(label, bool)
                                             or label < 0 for label in labels):
            raise ValueError(f"invalid result labels for query {index}")
        if len(set(labels)) != len(labels):
            raise ValueError(f"duplicate result labels for query {index}")
        if "distances" in row and (len(row["distances"]) != len(labels)
                                   or any(not math.isfinite(float(x)) for x in row["distances"])):
            raise ValueError(f"invalid distances for query {index}")
        result[index] = labels
    return result


def percentile(values, fraction):
    values = sorted(values)
    index = (len(values) - 1) * fraction
    lower = int(index)
    upper = min(lower + 1, len(values) - 1)
    return values[lower] + (values[upper] - values[lower]) * (index - lower)


def score_results(results_path, gt_path, k=10, min_recall=0.9, bootstrap_samples=1000,
                  seed=20261117, paired_score=None, max_paired_delta=0.005,
                  manifest=None, split=None):
    if k <= 0 or not 0 <= min_recall <= 1 or not 0 <= max_paired_delta <= 1:
        raise ValueError("invalid score gate")
    truths = read_gt(gt_path)
    if any(len(row) < k or len(set(row[:k])) != k for row in truths):
        raise ValueError("GT has insufficient or duplicate top-K labels")
    results = load_results(results_path)
    if set(results) != set(range(len(truths))):
        raise ValueError("query results must cover every GT query exactly once")
    samples = []
    for index, truth in enumerate(truths):
        hits = len(set(results[index][:k]) & set(truth[:k]))
        samples.append(dict(query_index=index, hits=hits, denominator=k, recall=hits / k))
    recall = sum(row["hits"] for row in samples) / (k * len(samples))
    scores = [row["recall"] for row in samples]
    rng = random.Random(seed)
    boot = [sum(scores[rng.randrange(len(scores))] for _ in scores) / len(scores)
            for _ in range(bootstrap_samples)]
    result = dict(schema_version=1, status="pass" if recall + 1e-12 >= min_recall else "fail",
                  recall_k=k, queries=len(samples), hits=sum(row["hits"] for row in samples),
                  denominator=k * len(samples), recall_at_k=recall, minimum_recall=min_recall,
                  absolute_gate_passed=recall + 1e-12 >= min_recall,
                  query_recall_p10=percentile(scores, 0.1), query_recall_p50=percentile(scores, 0.5),
                  bootstrap_95_ci=[percentile(boot, 0.025), percentile(boot, 0.975)] if boot else None,
                  bootstrap_samples=bootstrap_samples, bootstrap_seed=seed,
                  results_sha256=sha256(results_path), ground_truth_sha256=sha256(gt_path),
                  samples=samples)
    if manifest is not None:
        if split not in manifest["splits"] or manifest["splits"][split]["count"] != len(samples):
            raise ValueError("score manifest/split mismatch")
        ids = manifest["splits"][split]["original_query_ids"]
        for sample, original in zip(samples, ids):
            sample["original_query_id"] = original
        result["query_identity_sha256"] = canonical_hash(ids)
        result["query_vectors_sha256"] = manifest["splits"][split]["vectors_sha256"]
    if paired_score is not None:
        other = paired_score
        for key in ("recall_k", "queries", "ground_truth_sha256", "query_identity_sha256",
                    "query_vectors_sha256"):
            if other.get(key) != result.get(key):
                raise ValueError(f"paired score identity mismatch: {key}")
        delta = recall - other["recall_at_k"]
        paired_pass = abs(delta) <= max_paired_delta + 1e-12 and other["absolute_gate_passed"]
        result.update(paired_delta=delta, max_paired_delta=max_paired_delta,
                      paired_gate_passed=paired_pass)
        if not paired_pass:
            result["status"] = "fail"
    return result


def f32(value):
    return struct.unpack("<f", struct.pack("<f", value))[0]


class VectorFile:
    def __init__(self, path, kind, expected_count=None, expected_dim=None):
        self.info = vector_info(path, kind, expected_count, expected_dim)
        self.kind = kind
        self.dim = self.info["dim"]
        self.count = self.info["count"]
        self.row_bytes = self.dim * (4 if kind == "f32" else 1)
        self.stream = Path(path).open("rb")
        self.row = lru_cache(maxsize=100000)(self._row)

    def _row(self, index):
        if not 0 <= index < self.count:
            raise ValueError("vector row index out of bounds")
        self.stream.seek(8 + index * self.row_bytes)
        raw = self.stream.read(self.row_bytes)
        if len(raw) != self.row_bytes:
            raise ValueError("truncated vector file")
        values = tuple(raw) if self.kind == "u8" else struct.unpack("<" + str(self.dim) + "f", raw)
        if not all(math.isfinite(x) for x in values):
            raise ValueError("non-finite vector data")
        return values

    def block(self, start, count):
        if start < 0 or count < 0 or start + count > self.count:
            raise ValueError("vector block out of bounds")
        self.stream.seek(8 + start * self.row_bytes)
        raw = self.stream.read(count * self.row_bytes)
        if len(raw) != count * self.row_bytes:
            raise ValueError("truncated vector block")
        return raw

    def close(self):
        self.stream.close()
        self.row.cache_clear()


class DistanceEngine:
    def __init__(self, metric, dim, kind, numpy_enabled=True):
        if metric not in ("l2", "ip") or (metric == "ip" and kind != "f32"):
            raise ValueError("unsupported metric/vector-kind combination")
        self.metric, self.dim, self.kind = metric, dim, kind
        self.np = None
        if numpy_enabled:
            try:
                import numpy
                self.np = numpy
            except ImportError:
                pass

    def distance(self, query, vector):
        acc = 0.0
        if self.metric == "l2":
            for left, right in zip(query, vector):
                delta = f32(left - right)
                acc = f32(acc + f32(delta * delta))
            return acc
        for left, right in zip(query, vector):
            acc = f32(acc + f32(left * right))
        return f32(1.0 - acc)

    def distances(self, query, vectors):
        if self.np is None:
            return [self.distance(query, vector) for vector in vectors]
        np = self.np
        values = np.asarray(vectors, dtype=np.float32).reshape((-1, self.dim))
        return self.matrix_distances(query, values)

    def matrix_distances(self, query, values):
        np = self.np
        if not np.all(np.isfinite(values)):
            raise ValueError("non-finite vector data")
        acc = np.zeros(len(values), dtype=np.float32)
        query = np.asarray(query, dtype=np.float32)
        for col in range(self.dim):
            if self.metric == "l2":
                delta = np.subtract(query[col], values[:, col], dtype=np.float32)
                term = np.multiply(delta, delta, dtype=np.float32)
            else:
                term = np.multiply(query[col], values[:, col], dtype=np.float32)
            np.add(acc, term, out=acc)
        if self.metric == "ip":
            np.subtract(np.float32(1), acc, out=acc)
        if not np.all(np.isfinite(acc)):
            raise ValueError("non-finite distance")
        return acc

    def block_distances(self, query, raw):
        if self.np is not None:
            dtype = self.np.dtype("<f4") if self.kind == "f32" else self.np.uint8
            values = self.np.frombuffer(raw, dtype=dtype).reshape((-1, self.dim)).astype(self.np.float32)
            return self.matrix_distances(query, values)
        fmt = "<" + str(self.dim) + ("f" if self.kind == "f32" else "B")
        return [self.distance(query, vector) for vector in struct.iter_unpack(fmt, raw)]


def read_replacements(trace_path, update_count, base_count, expected_updates=None):
    lines = [json.loads(line) for line in Path(trace_path).read_text().splitlines() if line.strip()]
    if not lines or expected_updates is not None and len(lines) != expected_updates:
        raise ValueError("empty or incomplete update trace")
    sequences = [row["update_sequence"] for row in lines]
    if set(sequences) != set(range(len(lines))) or len(set(sequences)) != len(lines):
        raise ValueError("update trace must contain one complete prefix of update_sequence")
    if any(row.get("success") is not True for row in lines):
        raise ValueError("cannot certify dynamic truth for a failed update trace")
    replacements = {}
    for row in lines:
        target, index = row["target_label"], row["update_vector_index"]
        if not isinstance(target, int) or not 0 <= target < base_count:
            raise ValueError("replacement target label is outside original base")
        if not isinstance(index, int) or not 0 <= index < update_count:
            raise ValueError("replacement vector index out of bounds")
        if target in replacements:
            if "commit_sequence" not in row or "commit_sequence" not in replacements[target]:
                raise ValueError("repeated labels require explicit commit_sequence")
            if row["commit_sequence"] <= replacements[target]["commit_sequence"]:
                continue
        replacements[target] = row
    return replacements


def exact_active_neighbors(query, base, replacements, update_vectors, engine, k, block_rows=65536):
    overwritten = sorted(replacements)
    top = []
    for start in range(0, base.count, block_rows):
        count = min(block_rows, base.count - start)
        distances = engine.block_distances(query, base.block(start, count))
        if engine.np is not None:
            np = engine.np
            for label in overwritten[bisect.bisect_left(overwritten, start):
                                     bisect.bisect_left(overwritten, start + count)]:
                distances[label - start] = np.inf
            finite = np.flatnonzero(np.isfinite(distances))
            if len(finite) > k:
                threshold = np.partition(distances[finite], k - 1)[k - 1]
                finite = finite[distances[finite] <= threshold]
                finite = finite[np.lexsort((finite, distances[finite]))[:k]]
            candidates = ((float(distances[i]), start + int(i)) for i in finite)
        else:
            candidates = ((distance, start + i) for i, distance in enumerate(distances)
                          if start + i not in replacements)
        for distance, label in candidates:
            entry = (-float(distance), -label)
            if len(top) < k:
                heapq.heappush(top, entry)
            elif entry > top[0]:
                heapq.heapreplace(top, entry)
    candidates = [(-distance, -label) for distance, label in top]
    vectors = [update_vectors.row(row["update_vector_index"]) for row in replacements.values()]
    for label, distance in zip(replacements, engine.distances(query, vectors)):
        candidates.append((float(distance), label))
    return sorted(candidates)[:k]


def incremental_active_neighbors(query, base_truth, base, replacements, update_vectors, engine,
                                 k, block_rows=65536):
    if len(set(base_truth)) != len(base_truth) or any(not 0 <= x < base.count for x in base_truth):
        raise ValueError("invalid base GT prefix")
    surviving = [label for label in base_truth if label not in replacements]
    if len(surviving) < k:
        return exact_active_neighbors(query, base, replacements, update_vectors, engine, k, block_rows), True
    distances = engine.distances(query, [base.row(label) for label in base_truth])
    base_distances = {label: float(distance) for label, distance in zip(base_truth, distances)}
    candidates = sorted((base_distances[label], label) for label in surviving)
    # The supplied prefix may omit labels tied at its boundary. Do not infer
    # deterministic label ordering from an arbitrary truncated tie group.
    boundary = max(base_distances.values())
    kth = candidates[k - 1][0]
    tolerance = 1e-6 * max(1.0, abs(boundary), abs(kth))
    if len(base_truth) < base.count and boundary - kth <= tolerance:
        return exact_active_neighbors(query, base, replacements, update_vectors, engine, k, block_rows), True
    replacement_vectors = [update_vectors.row(row["update_vector_index"])
                           for row in replacements.values()]
    for label, distance in zip(replacements, engine.distances(query, replacement_vectors)):
        candidates.append((float(distance), label))
    return sorted(candidates)[:k], False


def dynamic_ground_truth(manifest_path, split, trace_path, output, metadata_out,
                         k=10, expected_updates=None, numpy_enabled=True, block_rows=65536):
    manifest = json.loads(Path(manifest_path).read_text())
    if split not in ("calibration", "validation") or split not in manifest["splits"]:
        raise ValueError("dynamic GT split must be calibration or validation")
    if k <= 0 or k > manifest["base"]["count"] or block_rows <= 0:
        raise ValueError("invalid dynamic GT K/block size")
    data = manifest["splits"][split]
    update = manifest["splits"]["update"]
    for path_key, hash_key in (("vectors", "vectors_sha256"), ("ground_truth", "ground_truth_sha256")):
        if sha256(data[path_key]) != data[hash_key]:
            raise ValueError("prepared query/GT file changed")
    if sha256(update["vectors"]) != update["vectors_sha256"]:
        raise ValueError("prepared update file changed")
    base_info = manifest["base"]
    stat = Path(base_info["path"]).stat()
    if stat.st_size != base_info["size_bytes"] or stat.st_mtime_ns != base_info["mtime_ns"]:
        raise ValueError("base identity changed since preparation; revalidate SHA-256")
    kind, metric = manifest["config"]["kind"], manifest["config"]["metric"]
    base = VectorFile(base_info["path"], kind, base_info["count"], base_info["dim"])
    queries = VectorFile(data["vectors"], kind, data["count"], base.dim)
    updates = VectorFile(update["vectors"], kind, update["count"], base.dim)
    try:
        replacements = read_replacements(trace_path, updates.count, base.count, expected_updates)
        engine = DistanceEngine(metric, base.dim, kind, numpy_enabled)
        truths = read_gt(data["ground_truth"])
        rows, fallback = [], []
        for i, truth in enumerate(truths):
            neighbors, scanned = incremental_active_neighbors(
                queries.row(i), truth, base, replacements, updates, engine, k, block_rows)
            if len(neighbors) != k:
                raise ValueError("dynamic GT returned fewer than K active labels")
            rows.append([label for distance, label in neighbors])
            if scanned:
                fallback.append(i)
        write_gt(output, rows)
        active = {str(label): row["update_vector_index"] for label, row in replacements.items()}
        metadata = dict(schema_version=1, status="complete", dataset=manifest["dataset"], split=split,
                        recall_k=k, queries=len(rows), active_base_count=base.count - len(replacements),
                        active_replacement_count=len(replacements), active_count=base.count,
                        mapping_sha256=canonical_hash(active), update_trace_sha256=sha256(trace_path),
                        base_sha256=base_info["sha256"],
                        query_vectors_sha256=data["vectors_sha256"],
                        source_ground_truth_sha256=data["ground_truth_sha256"],
                        ground_truth_sha256=sha256(output), full_scan_queries=fallback,
                        distance_arithmetic="sequential float32 multiply/add; IP=1-dot; ties by external label",
                        distance_backend="numpy" if engine.np is not None else "stdlib",
                        exactness_precondition="Original base top-L is exact for the recorded base N and metric; boundary/insufficient-prefix queries scanned in full.")
        write_json(metadata_out, metadata)
        return metadata
    finally:
        base.close()
        queries.close()
        updates.close()


def verify_base_ground_truth(manifest_path, split, sample_queries=2, k=10,
                             numpy_enabled=True, block_rows=65536,
                             distance_rtol=1e-5, distance_atol=1e-6):
    """Check an a-priori prefix of held-out identities by exact full-base scan.

    ID differences at a distance tie are reported separately. A supplied top-K
    must include all strictly closer exact winners and no point beyond the exact
    Kth-distance tolerance; this does not excuse a genuinely wrong GT scope.
    """
    manifest = json.loads(Path(manifest_path).read_text())
    if split not in ("calibration", "validation") or k <= 0 or sample_queries <= 0:
        raise ValueError("invalid source-GT verification parameters")
    data, info = manifest["splits"][split], manifest["base"]
    if sample_queries > data["count"] or k > info["count"] or block_rows <= 0:
        raise ValueError("GT verification sample/K/block exceeds available data")
    if min(distance_atol, distance_rtol) < 0:
        raise ValueError("distance tolerances must be nonnegative")
    stat = Path(info["path"]).stat()
    if stat.st_size != info["size_bytes"] or stat.st_mtime_ns != info["mtime_ns"]:
        raise ValueError("base identity changed since split preparation")
    for key, hash_key in (("vectors", "vectors_sha256"), ("ground_truth", "ground_truth_sha256")):
        if sha256(data[key]) != data[hash_key]:
            raise ValueError("prepared query/GT identity changed")
    kind, metric = manifest["config"]["kind"], manifest["config"]["metric"]
    base = VectorFile(info["path"], kind, info["count"], info["dim"])
    queries = VectorFile(data["vectors"], kind, data["count"], info["dim"])
    try:
        engine = DistanceEngine(metric, info["dim"], kind, numpy_enabled)
        truths = read_gt(data["ground_truth"])
        samples = []
        for index in range(sample_queries):
            truth = truths[index][:k]
            if len(truth) != k or len(set(truth)) != k or any(not 0 <= x < base.count for x in truth):
                raise ValueError("source GT top-K invalid for recorded base")
            query = queries.row(index)
            exact = exact_active_neighbors(query, base, {}, None, engine, k, block_rows)
            threshold = exact[-1][0]
            tolerance = distance_atol + distance_rtol * abs(threshold)
            supplied_distances = [float(x) for x in engine.distances(query, [base.row(x) for x in truth])]
            strict = {label for distance, label in exact if distance < threshold - tolerance}
            passed = (strict.issubset(set(truth))
                      and max(supplied_distances) <= threshold + tolerance)
            samples.append(dict(query_index=index,
                                original_query_id=data["original_query_ids"][index],
                                status="pass" if passed else "fail",
                                strict_label_set_match=set(truth) == {x[1] for x in exact},
                                original_labels=truth, original_distances=supplied_distances,
                                exact_labels=[x[1] for x in exact],
                                exact_distances=[x[0] for x in exact],
                                kth_distance=threshold, distance_tolerance=tolerance))
        return dict(schema_version=1, status="pass" if all(x["status"] == "pass" for x in samples) else "fail",
                    dataset=manifest["dataset"], split=split, recall_k=k,
                    sample_queries=sample_queries,
                    selection="first compact rows from the seed-frozen query identity manifest",
                    base_count=base.count, base_sha256=info["sha256"],
                    query_vectors_sha256=data["vectors_sha256"],
                    source_ground_truth_sha256=data["ground_truth_sha256"],
                    distance_backend="numpy" if engine.np is not None else "stdlib",
                    distance_arithmetic="sequential float32 multiply/add; distance-aware boundary-tie validation",
                    distance_atol=distance_atol, distance_rtol=distance_rtol, samples=samples,
                    coverage_note="Full-base exact scans for the recorded sample only; not exhaustive validation of every GT row.")
    finally:
        base.close()
        queries.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare", help="freeze disjoint query/update files and GT rows")
    prepare.add_argument("--dataset", choices=DATASETS, required=True)
    prepare.add_argument("--dataset-root", type=Path, required=True)
    prepare.add_argument("--output-dir", type=Path, required=True)
    prepare.add_argument("--seed", type=int, default=20261117)
    score = commands.add_parser("score", help="score complete active-label query results")
    score.add_argument("--results", type=Path, required=True)
    score.add_argument("--ground-truth", type=Path, required=True)
    score.add_argument("--output", type=Path, required=True)
    score.add_argument("--recall-k", type=int, default=10)
    score.add_argument("--min-recall", type=float, default=0.9)
    score.add_argument("--bootstrap-samples", type=int, default=1000)
    score.add_argument("--seed", type=int, default=20261117)
    score.add_argument("--paired-score", type=Path)
    score.add_argument("--max-paired-delta", type=float, default=0.005)
    score.add_argument("--manifest", type=Path)
    score.add_argument("--split", choices=("calibration", "validation"))
    dynamic = commands.add_parser("dynamic-gt", help="compute active snapshot GT from committed updates")
    dynamic.add_argument("--manifest", type=Path, required=True)
    dynamic.add_argument("--split", choices=("calibration", "validation"), required=True)
    dynamic.add_argument("--update-trace", type=Path, required=True)
    dynamic.add_argument("--output", type=Path, required=True)
    dynamic.add_argument("--metadata-out", type=Path, required=True)
    dynamic.add_argument("--recall-k", type=int, default=10)
    dynamic.add_argument("--expected-updates", type=int)
    dynamic.add_argument("--scan-block-rows", type=int, default=65536)
    dynamic.add_argument("--no-numpy", action="store_true")
    verify = commands.add_parser("verify-gt", help="verify sampled source GT by exact full-base scans")
    verify.add_argument("--manifest", type=Path, required=True)
    verify.add_argument("--split", choices=("calibration", "validation"), default="calibration")
    verify.add_argument("--sample-queries", type=int, default=2)
    verify.add_argument("--recall-k", type=int, default=10)
    verify.add_argument("--scan-block-rows", type=int, default=65536)
    verify.add_argument("--distance-rtol", type=float, default=1e-5)
    verify.add_argument("--distance-atol", type=float, default=1e-6)
    verify.add_argument("--no-numpy", action="store_true")
    verify.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "prepare":
            result = prepare_dataset(args.dataset, args.dataset_root, args.output_dir, args.seed)
            print(json.dumps(dict(status="prepared", dataset=result["dataset"],
                                  manifest=str(args.output_dir / "split-manifest.json"),
                                  counts={k: v["count"] for k, v in result["splits"].items()})))
        elif args.command == "score":
            if args.bootstrap_samples < 0:
                raise ValueError("bootstrap sample count must be nonnegative")
            manifest = json.loads(args.manifest.read_text()) if args.manifest else None
            other = json.loads(args.paired_score.read_text()) if args.paired_score else None
            result = score_results(args.results, args.ground_truth, args.recall_k, args.min_recall,
                                   args.bootstrap_samples, args.seed, other, args.max_paired_delta,
                                   manifest, args.split)
            write_json(args.output, result)
            print(json.dumps({key: result[key] for key in ("status", "queries", "recall_k", "recall_at_k")}))
            return 0 if result["status"] == "pass" else 1
        elif args.command == "dynamic-gt":
            result = dynamic_ground_truth(args.manifest, args.split, args.update_trace,
                                          args.output, args.metadata_out, args.recall_k,
                                          args.expected_updates, not args.no_numpy,
                                          args.scan_block_rows)
            print(json.dumps(dict(status="complete", queries=result["queries"],
                                  full_scan_queries=len(result["full_scan_queries"]),
                                  distance_backend=result["distance_backend"])))
        else:
            result = verify_base_ground_truth(args.manifest, args.split, args.sample_queries,
                                              args.recall_k, not args.no_numpy,
                                              args.scan_block_rows, args.distance_rtol,
                                              args.distance_atol)
            write_json(args.output, result)
            print(json.dumps(dict(status=result["status"], sample_queries=result["sample_queries"],
                                  distance_backend=result["distance_backend"])))
            return 0 if result["status"] == "pass" else 1
        return 0
    except (ValueError, OSError, KeyError, struct.error, OverflowError) as exc:
        print(f"baseline-quality: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
