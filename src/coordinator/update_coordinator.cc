#include <rados/librados.hpp>

#include "include/ceph_assert.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <cstring>
#include <map>
#include <mutex>
#include <queue>
#include <random>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "nsvu/placement.hpp"
#include "nsvu/protocol.hpp"

namespace {

using ghnsw::AdjacencyBlob;
using ghnsw::CasGlobalMetaRequest;
using ghnsw::CasLabelRequest;
using ghnsw::DistanceBatchReply;
using ghnsw::DistanceBatchRequest;
using ghnsw::EdgePatchBatchRequest;
using ghnsw::EmptyRequest;
using ghnsw::FinalizeInsertRequest;
using ghnsw::GetAdjBatchReply;
using ghnsw::GetGlobalMetaReply;
using ghnsw::GetVectorBatchReply;
using ghnsw::GlobalMeta;
using ghnsw::IdBatchRequest;
using ghnsw::LabelBatchRequest;
using ghnsw::LabelUpdateBatchRequest;
using ghnsw::LookupLabelBatchReply;
using ghnsw::MarkNodeStaleRequest;
using ghnsw::Opcode;
using ghnsw::ReserveInsertReply;
using ghnsw::ReserveInsertRequest;
using ghnsw::RequestEnvelope;
using ghnsw::SetAdjacencyBatchRequest;
using ghnsw::StatusReply;
using ghnsw::StoreVectorRequest;
using ghnsw::TimedNoopReply;
using ghnsw::VectorRef;

struct Config {
  std::string mode = "build";
  std::string input;
  std::string update_input;
  std::string input_format = "u8bin";
  std::string update_input_format = "u8bin";
  std::string keyring;
  std::string meta_pool = "nsvu_meta";
  std::string owner_pool_prefix = "nsvu_owner_";
  std::string data_oid = "hnsw.owner.data";
  std::string meta_oid = "hnsw.global.meta";
  std::string metrics_out;
  std::string progress_out;
  std::string ground_truth;
  uint64_t num_vectors = 100000;
  uint64_t num_updates = 100;
  uint64_t update_offset = 100000;
  uint64_t target_start = 0;
  uint32_t dim = 128;
  uint32_t owners = 5;
  uint64_t points_per_object = 250000;
  uint32_t M = 8;
  uint32_t ef = 32;
  uint32_t seed = 42;
  uint32_t vector_kind = ghnsw::kVectorKindU8;
  uint32_t metric = ghnsw::kMetricL2;
  std::string distance_mode = "osd";
  bool distance_split_probe = false;
  double distance_probe_interval_seconds = 1.0;
  uint64_t time_limit_seconds = 0;
  uint32_t update_parallelism = 1;
  uint32_t osd_op_timeout_seconds = 45;
  uint32_t osd_op_retry_limit = 3;
  uint32_t recall_k = 10;
};

struct OperationMetrics {
  uint64_t calls = 0;
  uint64_t request_bytes = 0;
  uint64_t reply_bytes = 0;
  double roundtrip_seconds = 0.0;
};

struct RecallSample {
  uint64_t update_sequence = 0;
  uint64_t hits = 0;
  uint64_t denominator = 0;
};

struct Metrics {
  std::string mode;
  uint64_t vectors_processed = 0;
  double total_seconds = 0.0;
  double load_seconds = 0.0;
  double graph_seconds = 0.0;
  uint64_t remote_vector_calls = 0;
  uint64_t remote_adj_calls = 0;
  uint64_t remote_distance_calls = 0;
  uint64_t remote_noop_calls = 0;
  uint64_t remote_patch_calls = 0;
  uint64_t remote_vector_bytes = 0;
  uint64_t remote_candidates_scored = 0;
  uint64_t remote_adj_nodes = 0;
  uint64_t total_patched_nodes = 0;
  uint64_t total_neighbor_links = 0;
  uint64_t stale_marks = 0;
  uint64_t meta_cas_retries = 0;
  uint64_t label_cas_conflicts = 0;
  uint64_t failed_updates = 0;
  uint64_t failed_update_timeouts = 0;
  uint64_t failed_update_conflicts = 0;
  uint64_t failed_update_not_found = 0;
  uint64_t failed_update_protocol = 0;
  uint64_t failed_update_other = 0;
  uint64_t total_cls_exec_calls = 0;
  uint64_t total_raw_rados_calls = 0;
  uint64_t rados_exec_retries = 0;
  uint64_t global_meta_cas_calls = 0;
  uint64_t label_cas_calls = 0;
  uint64_t cls_request_bytes = 0;
  uint64_t cls_reply_bytes = 0;
  uint64_t raw_rados_request_bytes = 0;
  uint64_t raw_rados_reply_bytes = 0;
  uint64_t logical_distance_query_bytes = 0;
  uint64_t distance_batches = 0;
  uint64_t max_candidates_per_distance_batch = 0;
  uint64_t unique_distance_objects_sum = 0;
  uint64_t max_unique_distance_objects = 0;
  std::map<uint64_t, uint64_t> distance_batch_size_histogram;
  uint64_t frontier_windows = 0;
  uint64_t frontier_nodes_selected = 0;
  uint64_t distance_candidates_before_dedupe = 0;
  uint64_t distance_candidates_after_dedupe = 0;
  uint64_t update_attempts_observed = 0;
  uint64_t unique_data_objects_sum = 0;
  uint64_t unique_data_pgs_sum = 0;
  uint64_t unique_owner_shards_sum = 0;
  uint64_t max_unique_data_objects = 0;
  uint64_t max_unique_data_pgs = 0;
  uint64_t max_unique_owner_shards = 0;
  uint64_t cross_owner_neighbor_links = 0;
  uint64_t recall_evaluated_updates = 0;
  uint64_t recall_hits = 0;
  uint64_t recall_denominator = 0;
  std::map<uint64_t, RecallSample> recall_samples;
  std::map<std::string, OperationMetrics> operation_metrics;
  std::map<std::string, OperationMetrics> raw_operation_metrics;
  std::map<std::string, uint64_t> failure_operations;
  std::set<std::string> current_data_objects;
  std::set<std::string> current_data_pgs;
  std::set<uint32_t> current_owner_shards;
  std::set<std::string> current_distance_objects;
  uint64_t time_limit_seconds = 0;
  uint32_t update_parallelism = 1;
  bool stopped_by_time_limit = false;
  double throughput_updates_per_sec = 0.0;
  double avg_update_latency_ms = 0.0;
  double p50_update_latency_ms = 0.0;
  double p95_update_latency_ms = 0.0;
  double p99_update_latency_ms = 0.0;
  double lookup_old_seconds = 0.0;
  double mark_stale_seconds = 0.0;
  double store_vector_seconds = 0.0;
  double remote_vector_seconds = 0.0;
  double meta_read_seconds = 0.0;
  double graph_search_seconds = 0.0;
  double graph_search_distance_seconds = 0.0;
  double remote_distance_seconds = 0.0;
  double distance_fetch_rpc_seconds = 0.0;
  double distance_local_compute_seconds = 0.0;
  double distance_compute_node_unaccounted_seconds = 0.0;
  double distance_cls_total_seconds = 0.0;
  double distance_vector_ref_seconds = 0.0;
  double distance_payload_read_seconds = 0.0;
  double distance_compute_seconds = 0.0;
  double distance_cls_accounted_seconds = 0.0;
  double distance_cls_unaccounted_seconds = 0.0;
  double distance_roundtrip_queue_seconds = 0.0;
  double distance_noop_roundtrip_seconds = 0.0;
  double distance_noop_cls_total_seconds = 0.0;
  double distance_network_roundtrip_est_seconds = 0.0;
  double distance_osd_queue_est_seconds = 0.0;
  double distance_unaccounted_seconds = 0.0;
  double patch_prepare_seconds = 0.0;
  double patch_prepare_distance_seconds = 0.0;
  double adjacency_patch_seconds = 0.0;
  double set_new_adjacency_seconds = 0.0;
  double global_meta_update_seconds = 0.0;
  double full_graph_search_exclusive_seconds = 0.0;
  double adjacency_patch_exclusive_seconds = 0.0;
  double accounted_update_seconds = 0.0;
  double other_update_seconds = 0.0;
};

class ProtocolError : public std::runtime_error {
 public:
  explicit ProtocolError(const std::string& what) : std::runtime_error(what) {}
};

class CephOperationError : public std::runtime_error {
 public:
  CephOperationError(const std::string& what, int code)
      : std::runtime_error(what + " failed: " + std::to_string(code)),
        operation_(what),
        code_(code) {}

  int code() const { return code_; }
  const std::string& operation() const { return operation_; }

 private:
  std::string operation_;
  int code_;
};

void begin_update_observation(Metrics* metrics) {
  metrics->current_data_objects.clear();
  metrics->current_data_pgs.clear();
  metrics->current_owner_shards.clear();
  metrics->current_distance_objects.clear();
}

void finish_update_observation(Metrics* metrics) {
  const uint64_t objects = metrics->current_data_objects.size();
  const uint64_t pgs = metrics->current_data_pgs.size();
  const uint64_t owners = metrics->current_owner_shards.size();
  const uint64_t distance_objects = metrics->current_distance_objects.size();
  metrics->update_attempts_observed++;
  metrics->unique_data_objects_sum += objects;
  metrics->unique_data_pgs_sum += pgs;
  metrics->unique_owner_shards_sum += owners;
  metrics->max_unique_data_objects = std::max(metrics->max_unique_data_objects, objects);
  metrics->max_unique_data_pgs = std::max(metrics->max_unique_data_pgs, pgs);
  metrics->max_unique_owner_shards = std::max(metrics->max_unique_owner_shards, owners);
  metrics->unique_distance_objects_sum += distance_objects;
  metrics->max_unique_distance_objects =
      std::max(metrics->max_unique_distance_objects, distance_objects);
  metrics->current_data_objects.clear();
  metrics->current_data_pgs.clear();
  metrics->current_owner_shards.clear();
  metrics->current_distance_objects.clear();
}

void record_update_failure(Metrics* metrics, const std::exception& error) {
  metrics->failed_updates++;
  if (dynamic_cast<const ProtocolError*>(&error) != nullptr) {
    metrics->failed_update_protocol++;
    metrics->failure_operations["protocol_decode"]++;
    return;
  }
  const auto* ceph_error = dynamic_cast<const CephOperationError*>(&error);
  if (ceph_error == nullptr) {
    metrics->failed_update_other++;
    metrics->failure_operations["other"]++;
    return;
  }
  metrics->failure_operations[ceph_error->operation()]++;
  switch (ceph_error->code()) {
    case -ETIMEDOUT:
    case -ETIME:
      metrics->failed_update_timeouts++;
      break;
    case -EAGAIN:
    case -EBUSY:
      metrics->failed_update_conflicts++;
      break;
    case -ENOENT:
      metrics->failed_update_not_found++;
      break;
    case -EBADMSG:
    case -EPROTONOSUPPORT:
      metrics->failed_update_protocol++;
      break;
    default:
      metrics->failed_update_other++;
      break;
  }
}

template <typename T>
void decode_or_die(const ceph::bufferlist& bl, T* out, const std::string& what) {
  auto it = bl.cbegin();
  try {
    out->decode(it);
  } catch (const ceph::buffer::error&) {
    throw ProtocolError("decode failed: " + what);
  }
}

template <typename T>
void decode_cls_reply_or_die(
    const ceph::bufferlist& bl,
    Opcode expected_opcode,
    T* out,
    const std::string& what) {
  RequestEnvelope response_envelope;
  const int result =
      ghnsw::DecodeResponse(bl, expected_opcode, &response_envelope, out);
  if (result < 0) {
    throw ProtocolError(
        "decode failed: " + what + " (" + std::to_string(result) + ")");
  }
}

template <typename T>
ceph::bufferlist encode_msg(const T& msg) {
  ceph::bufferlist bl;
  msg.encode(bl);
  return bl;
}

uint64_t next_protocol_request_id() {
  static std::atomic<uint64_t> next{1};
  return next.fetch_add(1, std::memory_order_relaxed);
}

template <typename T>
ceph::bufferlist encode_cls_request(
    Opcode opcode,
    const T& payload,
    uint64_t update_id = 0,
    uint64_t expected_version = 0,
    uint64_t placement_epoch = 0) {
  RequestEnvelope envelope;
  envelope.opcode = static_cast<uint32_t>(opcode);
  envelope.request_id = next_protocol_request_id();
  envelope.update_id = update_id;
  envelope.expected_version = expected_version;
  envelope.placement_epoch = placement_epoch;
  return ghnsw::EncodeRequest(envelope, payload);
}

double now_sec() {
  using Clock = std::chrono::steady_clock;
  static const auto start = Clock::now();
  const auto delta = Clock::now() - start;
  return std::chrono::duration_cast<std::chrono::duration<double>>(delta).count();
}

uint64_t make_update_id_seed() {
  const uint64_t timestamp = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count());
  std::random_device random;
  const uint64_t entropy =
      (static_cast<uint64_t>(random()) << 32) ^ static_cast<uint64_t>(random());
  const uint64_t seed = timestamp ^ entropy;
  return seed == 0 ? 1 : seed;
}

double percentile_ms(std::vector<double> values, double p) {
  if (values.empty()) {
    return 0.0;
  }
  std::sort(values.begin(), values.end());
  const double pos = p * static_cast<double>(values.size() - 1);
  const size_t lo = static_cast<size_t>(std::floor(pos));
  const size_t hi = static_cast<size_t>(std::ceil(pos));
  if (lo == hi) {
    return values[lo];
  }
  const double frac = pos - static_cast<double>(lo);
  return values[lo] * (1.0 - frac) + values[hi] * frac;
}

double nonnegative(double v) {
  return v > 0.0 ? v : 0.0;
}

float l2_u8_distance(const std::string& a, const std::string& b, uint32_t dim) {
  const uint8_t* pa = reinterpret_cast<const uint8_t*>(a.data());
  const uint8_t* pb = reinterpret_cast<const uint8_t*>(b.data());
  float acc = 0.0f;
  for (uint32_t i = 0; i < dim; ++i) {
    const float d = static_cast<float>(pa[i]) - static_cast<float>(pb[i]);
    acc += d * d;
  }
  return acc;
}

float l2_f32_distance(const std::string& a, const std::string& b, uint32_t dim) {
  const float* pa = reinterpret_cast<const float*>(a.data());
  const float* pb = reinterpret_cast<const float*>(b.data());
  float acc = 0.0f;
  for (uint32_t i = 0; i < dim; ++i) {
    const float d = pa[i] - pb[i];
    acc += d * d;
  }
  return acc;
}

float ip_f32_distance(const std::string& a, const std::string& b, uint32_t dim) {
  const float* pa = reinterpret_cast<const float*>(a.data());
  const float* pb = reinterpret_cast<const float*>(b.data());
  float dot = 0.0f;
  for (uint32_t i = 0; i < dim; ++i) {
    dot += pa[i] * pb[i];
  }
  return 1.0f - dot;
}

float compute_distance_local(
    const std::string& query,
    const std::string& vec,
    uint32_t dim,
    uint32_t vector_kind,
    uint32_t metric) {
  if (vector_kind == ghnsw::kVectorKindU8) {
    return l2_u8_distance(query, vec, dim);
  }
  if (vector_kind == ghnsw::kVectorKindF32 && metric == ghnsw::kMetricL2) {
    return l2_f32_distance(query, vec, dim);
  }
  if (vector_kind == ghnsw::kVectorKindF32 && metric == ghnsw::kMetricIP) {
    return ip_f32_distance(query, vec, dim);
  }
  return std::numeric_limits<float>::max();
}

void merge_update_metrics(Metrics* dst, const Metrics& src) {
  dst->vectors_processed += src.vectors_processed;
  dst->remote_vector_calls += src.remote_vector_calls;
  dst->remote_adj_calls += src.remote_adj_calls;
  dst->remote_distance_calls += src.remote_distance_calls;
  dst->remote_noop_calls += src.remote_noop_calls;
  dst->remote_patch_calls += src.remote_patch_calls;
  dst->remote_vector_bytes += src.remote_vector_bytes;
  dst->remote_candidates_scored += src.remote_candidates_scored;
  dst->remote_adj_nodes += src.remote_adj_nodes;
  dst->total_patched_nodes += src.total_patched_nodes;
  dst->total_neighbor_links += src.total_neighbor_links;
  dst->stale_marks += src.stale_marks;
  dst->meta_cas_retries += src.meta_cas_retries;
  dst->label_cas_conflicts += src.label_cas_conflicts;
  dst->failed_updates += src.failed_updates;
  dst->failed_update_timeouts += src.failed_update_timeouts;
  dst->failed_update_conflicts += src.failed_update_conflicts;
  dst->failed_update_not_found += src.failed_update_not_found;
  dst->failed_update_protocol += src.failed_update_protocol;
  dst->failed_update_other += src.failed_update_other;
  dst->total_cls_exec_calls += src.total_cls_exec_calls;
  dst->total_raw_rados_calls += src.total_raw_rados_calls;
  dst->rados_exec_retries += src.rados_exec_retries;
  dst->global_meta_cas_calls += src.global_meta_cas_calls;
  dst->label_cas_calls += src.label_cas_calls;
  dst->cls_request_bytes += src.cls_request_bytes;
  dst->cls_reply_bytes += src.cls_reply_bytes;
  dst->raw_rados_request_bytes += src.raw_rados_request_bytes;
  dst->raw_rados_reply_bytes += src.raw_rados_reply_bytes;
  dst->logical_distance_query_bytes += src.logical_distance_query_bytes;
  dst->distance_batches += src.distance_batches;
  dst->max_candidates_per_distance_batch = std::max(
      dst->max_candidates_per_distance_batch, src.max_candidates_per_distance_batch);
  dst->unique_distance_objects_sum += src.unique_distance_objects_sum;
  dst->max_unique_distance_objects = std::max(
      dst->max_unique_distance_objects, src.max_unique_distance_objects);
  for (const auto& [batch_size, count] : src.distance_batch_size_histogram) {
    dst->distance_batch_size_histogram[batch_size] += count;
  }
  dst->frontier_windows += src.frontier_windows;
  dst->frontier_nodes_selected += src.frontier_nodes_selected;
  dst->distance_candidates_before_dedupe +=
      src.distance_candidates_before_dedupe;
  dst->distance_candidates_after_dedupe +=
      src.distance_candidates_after_dedupe;
  dst->update_attempts_observed += src.update_attempts_observed;
  dst->unique_data_objects_sum += src.unique_data_objects_sum;
  dst->unique_data_pgs_sum += src.unique_data_pgs_sum;
  dst->unique_owner_shards_sum += src.unique_owner_shards_sum;
  dst->max_unique_data_objects = std::max(
      dst->max_unique_data_objects, src.max_unique_data_objects);
  dst->max_unique_data_pgs = std::max(
      dst->max_unique_data_pgs, src.max_unique_data_pgs);
  dst->max_unique_owner_shards = std::max(
      dst->max_unique_owner_shards, src.max_unique_owner_shards);
  dst->cross_owner_neighbor_links += src.cross_owner_neighbor_links;
  dst->recall_evaluated_updates += src.recall_evaluated_updates;
  dst->recall_hits += src.recall_hits;
  dst->recall_denominator += src.recall_denominator;
  for (const auto& [query_index, sample] : src.recall_samples) {
    auto found = dst->recall_samples.find(query_index);
    if (found == dst->recall_samples.end() ||
        sample.update_sequence < found->second.update_sequence) {
      dst->recall_samples[query_index] = sample;
    }
  }
  for (const auto& [operation, values] : src.operation_metrics) {
    auto& aggregate = dst->operation_metrics[operation];
    aggregate.calls += values.calls;
    aggregate.request_bytes += values.request_bytes;
    aggregate.reply_bytes += values.reply_bytes;
    aggregate.roundtrip_seconds += values.roundtrip_seconds;
  }
  for (const auto& [operation, values] : src.raw_operation_metrics) {
    auto& aggregate = dst->raw_operation_metrics[operation];
    aggregate.calls += values.calls;
    aggregate.request_bytes += values.request_bytes;
    aggregate.reply_bytes += values.reply_bytes;
    aggregate.roundtrip_seconds += values.roundtrip_seconds;
  }
  for (const auto& [operation, count] : src.failure_operations) {
    dst->failure_operations[operation] += count;
  }
  dst->lookup_old_seconds += src.lookup_old_seconds;
  dst->mark_stale_seconds += src.mark_stale_seconds;
  dst->store_vector_seconds += src.store_vector_seconds;
  dst->remote_vector_seconds += src.remote_vector_seconds;
  dst->meta_read_seconds += src.meta_read_seconds;
  dst->graph_search_seconds += src.graph_search_seconds;
  dst->graph_search_distance_seconds += src.graph_search_distance_seconds;
  dst->remote_distance_seconds += src.remote_distance_seconds;
  dst->distance_fetch_rpc_seconds += src.distance_fetch_rpc_seconds;
  dst->distance_local_compute_seconds += src.distance_local_compute_seconds;
  dst->distance_compute_node_unaccounted_seconds += src.distance_compute_node_unaccounted_seconds;
  dst->distance_cls_total_seconds += src.distance_cls_total_seconds;
  dst->distance_vector_ref_seconds += src.distance_vector_ref_seconds;
  dst->distance_payload_read_seconds += src.distance_payload_read_seconds;
  dst->distance_compute_seconds += src.distance_compute_seconds;
  dst->distance_noop_roundtrip_seconds += src.distance_noop_roundtrip_seconds;
  dst->distance_noop_cls_total_seconds += src.distance_noop_cls_total_seconds;
  dst->distance_network_roundtrip_est_seconds += src.distance_network_roundtrip_est_seconds;
  dst->distance_osd_queue_est_seconds += src.distance_osd_queue_est_seconds;
  dst->patch_prepare_seconds += src.patch_prepare_seconds;
  dst->patch_prepare_distance_seconds += src.patch_prepare_distance_seconds;
  dst->adjacency_patch_seconds += src.adjacency_patch_seconds;
  dst->set_new_adjacency_seconds += src.set_new_adjacency_seconds;
  dst->global_meta_update_seconds += src.global_meta_update_seconds;
}

std::vector<std::string> owner_pools(const Config& cfg) {
  std::vector<std::string> pools;
  for (uint32_t i = 0; i < cfg.owners; ++i) {
    pools.push_back(cfg.owner_pool_prefix + std::to_string(i));
  }
  return pools;
}

struct Neighbor {
  uint64_t id = 0;
  float dist = 0.0f;
};

struct TimedNoopSample {
  double roundtrip_seconds = 0.0;
  double cls_total_seconds = 0.0;
  double non_cls_seconds = 0.0;
  double baseline_non_cls_seconds = 0.0;
  double queue_est_seconds = 0.0;
};

constexpr const char* kMetaEnterPoint = "meta/enterpoint";
constexpr const char* kMetaMaxLevel = "meta/max_level";
constexpr const char* kMetaCurCount = "meta/cur_element_count";
constexpr const char* kMetaNextGlobal = "meta/next_global_id";
constexpr const char* kMetaVersion = "meta/version";
constexpr const char* kMetaM = "meta/M";
constexpr const char* kMetaEf = "meta/ef";
constexpr const char* kMetaDim = "meta/dim";
constexpr const char* kMetaVectorKind = "meta/vector_kind";
constexpr const char* kMetaMetric = "meta/metric";

template <typename T>
ceph::bufferlist encode_scalar(T value) {
  ceph::bufferlist bl;
  ceph::encode(value, bl);
  return bl;
}

template <typename T>
T decode_scalar_or_die(const ceph::bufferlist& bl, const std::string& what) {
  T value{};
  auto it = bl.cbegin();
  try {
    ceph::decode(value, it);
  } catch (const ceph::buffer::error&) {
    throw ProtocolError("decode failed: " + what);
  }
  return value;
}

std::map<std::string, ceph::bufferlist> encode_global_meta(const GlobalMeta& meta) {
  return {
      {kMetaEnterPoint, encode_scalar(meta.enterpoint)},
      {kMetaMaxLevel, encode_scalar(meta.max_level)},
      {kMetaCurCount, encode_scalar(meta.cur_element_count)},
      {kMetaNextGlobal, encode_scalar(meta.next_global_id)},
      {kMetaVersion, encode_scalar(meta.version)},
      {kMetaM, encode_scalar(meta.M)},
      {kMetaEf, encode_scalar(meta.ef)},
      {kMetaDim, encode_scalar(meta.dim)},
      {kMetaVectorKind, encode_scalar(meta.vector_kind)},
      {kMetaMetric, encode_scalar(meta.metric)},
  };
}

std::set<std::string> global_meta_keys() {
  return {
      kMetaEnterPoint,
      kMetaMaxLevel,
      kMetaCurCount,
      kMetaNextGlobal,
      kMetaVersion,
      kMetaM,
      kMetaEf,
      kMetaDim,
      kMetaVectorKind,
      kMetaMetric,
  };
}

// The experiment runner uses one Coordinator process with multiple worker
// facades. These shared locks preserve read-modify-write semantics across its
// librados clients without relying on server-side CLS code.
std::mutex& raw_meta_mutex() {
  static std::mutex mutex;
  return mutex;
}

std::mutex& raw_object_mutex(int64_t pool_id, const std::string& oid) {
  static std::array<std::mutex, 4096> mutexes;
  const size_t pool_hash = std::hash<int64_t>{}(pool_id);
  const size_t object_hash = std::hash<std::string>{}(oid);
  return mutexes[(pool_hash ^ (object_hash << 1)) % mutexes.size()];
}

class CephFacade {
 public:
  explicit CephFacade(const Config& cfg)
      : cfg_(cfg), placement_(cfg.owners, cfg.points_per_object) {}

  ghnsw::PhysicalLocation Resolve(uint64_t global_id) const {
    return placement_.Resolve(global_id);
  }

  void Connect() {
    int r = cluster_.init2("client.admin", "client", 0);
    if (r < 0) {
      throw std::runtime_error("cluster init failed");
    }
    if ((r = cluster_.conf_read_file("/etc/ceph/ceph.conf")) < 0) {
      throw std::runtime_error("conf_read_file failed");
    }
    if (!cfg_.keyring.empty() &&
        (r = cluster_.conf_set("keyring", cfg_.keyring.c_str())) < 0) {
      throw std::runtime_error("conf_set keyring failed");
    }
    // Every modifying CLS operation used by online updates is idempotent. A
    // bounded timeout can therefore be retried without treating an ambiguous
    // client response as a new mutation.
    const std::string osd_timeout = std::to_string(cfg_.osd_op_timeout_seconds);
    (void)cluster_.conf_set("rados_osd_op_timeout", osd_timeout.c_str());
    (void)cluster_.conf_set("rados_mon_op_timeout", "30");
    (void)cluster_.conf_set("client_mount_timeout", "30");
    if ((r = cluster_.connect()) < 0) {
      throw std::runtime_error("cluster connect failed");
    }

    if ((r = cluster_.ioctx_create(cfg_.meta_pool.c_str(), meta_ioctx_)) < 0) {
      throw std::runtime_error("open meta pool failed");
    }

    auto pools = owner_pools(cfg_);
    owner_ioctxs_.resize(pools.size());
    for (size_t i = 0; i < pools.size(); ++i) {
      if ((r = cluster_.ioctx_create(pools[i].c_str(), owner_ioctxs_[i])) < 0) {
        throw std::runtime_error("open owner pool failed: " + pools[i]);
      }
    }
  }

  ~CephFacade() {
    for (auto& ioctx : owner_ioctxs_) {
      ioctx.close();
    }
    meta_ioctx_.close();
    cluster_.shutdown();
  }

  int Exec(
      librados::IoCtx& ioctx,
      const std::string& oid,
      const char* method,
      ceph::bufferlist& in,
      ceph::bufferlist* out,
      Metrics* metrics) {
    if (cfg_.distance_mode == "compute") {
      throw ProtocolError(
          "raw-RADOS compute baseline attempted CLS method: " + std::string(method));
    }
    int r = 0;
    for (uint32_t attempt = 0; attempt <= cfg_.osd_op_retry_limit; ++attempt) {
      out->clear();
      const double t0 = now_sec();
      r = ioctx.exec(oid, "hnsw_global", method, in, *out);
      RecordExec(metrics, method, in, *out, now_sec() - t0);
      if (r != -ETIMEDOUT && r != -ETIME) {
        return r;
      }
      if (attempt == cfg_.osd_op_retry_limit) {
        break;
      }
      if (metrics) {
        metrics->rados_exec_retries++;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50 * (attempt + 1)));
    }
    return r;
  }

  void FetchVectorBatch(
      uint32_t owner,
      uint64_t chunk,
      const std::vector<uint64_t>& ids,
      std::unordered_map<uint64_t, std::string>* result,
      Metrics* metrics) {
    if (cfg_.distance_mode == "compute") {
      RawFetchVectorBatch(owner, chunk, ids, result, metrics);
      return;
    }
    IdBatchRequest req;
    req.global_ids = ids;
    ceph::bufferlist in =
        encode_cls_request(Opcode::kGetNodeVectorBatch, req), out;
    const double t0 = now_sec();
    int r = Exec(
        owner_ioctxs_[owner], ghnsw::OwnerDataOid(chunk),
        "get_node_vector_batch", in, &out, metrics);
    RecordDataTarget(metrics, owner, chunk);
    const double elapsed = now_sec() - t0;
    if (metrics) {
      metrics->remote_vector_calls++;
      metrics->remote_vector_seconds += elapsed;
    }
    if (r < 0) {
      throw CephOperationError("get_node_vector_batch", r);
    }
    GetVectorBatchReply reply;
    decode_cls_reply_or_die(
        out, Opcode::kGetNodeVectorBatch, &reply, "GetVectorBatchReply");
    uint64_t bytes = 0;
    for (auto& kv : reply.values) {
      bytes += kv.second.size();
      result->emplace(kv.first, std::move(kv.second));
    }
    if (metrics) {
      metrics->remote_vector_bytes += bytes;
    }
  }

  void StoreVector(
      uint64_t global_id,
      uint64_t external_label,
      const std::string& vec,
      uint32_t level,
      Metrics* metrics,
      bool update_label = true,
      uint64_t update_id = 0) {
    if (cfg_.distance_mode == "compute") {
      RawStoreVector(
          global_id, external_label, vec, level, metrics, update_label);
      return;
    }
    const double t0 = now_sec();
    StoreVectorRequest req;
    req.global_id = global_id;
    req.external_label = external_label;
    req.dim = cfg_.dim;
    req.vector_kind = cfg_.vector_kind;
    req.flags = ghnsw::kVectorFlagActive;
    req.level = level;
    req.vector_bytes = vec;
    const auto location = placement_.Resolve(global_id);
    const uint32_t owner = location.shard_id;
    const uint64_t chunk = location.chunk_id;
    ceph::bufferlist in = encode_cls_request(
        Opcode::kStoreVector, req, update_id, 0, location.placement_epoch), out;
    int r = Exec(
        owner_ioctxs_[owner], ghnsw::OwnerDataOid(chunk),
        "store_vector", in, &out, metrics);
    RecordDataTarget(metrics, owner, chunk);
    if (r < 0) {
      throw CephOperationError("store_vector", r);
    }
    if (update_label) {
      UpdateLabels({{external_label, global_id}}, metrics, update_id);
    }
    if (metrics) {
      metrics->store_vector_seconds += now_sec() - t0;
    }
  }

  void UpdateLabels(
      const std::vector<std::pair<uint64_t, uint64_t>>& labels,
      Metrics* metrics = nullptr,
      uint64_t update_id = 0) {
    if (cfg_.distance_mode == "compute") {
      RawUpdateLabels(labels, metrics);
      return;
    }
    std::map<uint32_t, std::vector<std::pair<uint64_t, uint64_t>>> groups;
    for (const auto& entry : labels) {
      groups[placement_.ResolveLabelShard(entry.first)].push_back(entry);
    }
    for (const auto& [owner, owner_labels] : groups) {
      LabelUpdateBatchRequest req;
      req.entries = owner_labels;
      ceph::bufferlist in =
          encode_cls_request(Opcode::kUpdateLabelBatch, req, update_id), out;
      int r = Exec(
          owner_ioctxs_[owner], ghnsw::OwnerMetaOid(),
          "update_label_batch", in, &out, metrics);
      if (r < 0) {
        throw CephOperationError("update_label_batch", r);
      }
    }
  }

  bool CasLabel(
      uint64_t external_label,
      bool expect_missing,
      uint64_t expected_global_id,
      uint64_t replacement_global_id,
      Metrics* metrics,
      uint64_t update_id = 0) {
    if (cfg_.distance_mode == "compute") {
      return RawCasLabel(
          external_label,
          expect_missing,
          expected_global_id,
          replacement_global_id,
          metrics);
    }
    const uint32_t owner = placement_.ResolveLabelShard(external_label);
    CasLabelRequest req;
    req.external_label = external_label;
    req.expected_global_id = expected_global_id;
    req.replacement_global_id = replacement_global_id;
    req.expect_missing = expect_missing;
    ceph::bufferlist in =
        encode_cls_request(Opcode::kCasLabel, req, update_id), out;
    if (metrics) {
      metrics->label_cas_calls++;
    }
    int r = Exec(
        owner_ioctxs_[owner], ghnsw::OwnerMetaOid(),
        "cas_label", in, &out, metrics);
    if (r == -EAGAIN) {
      return false;
    }
    if (r < 0) {
      throw CephOperationError("cas_label", r);
    }
    return true;
  }

  std::unordered_map<uint64_t, std::string> GetVectors(const std::vector<uint64_t>& ids, Metrics* metrics) {
    std::unordered_map<uint64_t, std::string> result;
    auto groups = GroupIds(ids);
    for (const auto& [owner_chunk, owner_ids] : groups) {
      FetchVectorBatch(owner_chunk.first, owner_chunk.second, owner_ids, &result, metrics);
    }
    return result;
  }

  std::unordered_map<uint64_t, uint64_t> LookupLabels(
      const std::vector<uint64_t>& labels,
      Metrics* metrics = nullptr) {
    if (cfg_.distance_mode == "compute") {
      return RawLookupLabels(labels, metrics);
    }
    std::unordered_map<uint64_t, uint64_t> result;
    std::map<uint32_t, std::vector<uint64_t>> groups;
    for (uint64_t label : labels) {
      groups[placement_.ResolveLabelShard(label)].push_back(label);
    }
    for (const auto& [owner, owner_labels] : groups) {
      LabelBatchRequest req;
      req.external_labels = owner_labels;
      ceph::bufferlist in =
          encode_cls_request(Opcode::kLookupLabelBatch, req), out;
      int r = Exec(
          owner_ioctxs_[owner], ghnsw::OwnerMetaOid(),
          "lookup_label_batch", in, &out, metrics);
      if (r < 0) {
        throw CephOperationError("lookup_label_batch", r);
      }
      LookupLabelBatchReply reply;
      decode_cls_reply_or_die(
          out, Opcode::kLookupLabelBatch, &reply, "LookupLabelBatchReply");
      for (const auto& kv : reply.values) {
        result.emplace(kv.first, kv.second);
      }
    }
    return result;
  }

  std::unordered_map<uint64_t, AdjacencyBlob> GetAdjacency(const std::vector<uint64_t>& ids, Metrics* metrics) {
    if (cfg_.distance_mode == "compute") {
      return RawGetAdjacency(ids, metrics);
    }
    std::unordered_map<uint64_t, AdjacencyBlob> result;
    auto groups = GroupIds(ids);
    for (const auto& [owner_chunk, owner_ids] : groups) {
      IdBatchRequest req;
      req.global_ids = owner_ids;
      ceph::bufferlist in =
          encode_cls_request(Opcode::kGetNodeAdjacencyBatch, req), out;
      metrics->remote_adj_calls++;
      metrics->remote_adj_nodes += owner_ids.size();
      int r = Exec(
          owner_ioctxs_[owner_chunk.first], ghnsw::OwnerDataOid(owner_chunk.second),
          "get_node_adjacency_batch", in, &out, metrics);
      RecordDataTarget(metrics, owner_chunk.first, owner_chunk.second);
      if (r < 0) {
        throw CephOperationError("get_node_adjacency_batch", r);
      }
      GetAdjBatchReply reply;
      decode_cls_reply_or_die(
          out, Opcode::kGetNodeAdjacencyBatch, &reply, "GetAdjBatchReply");
      for (auto& kv : reply.values) {
        result.emplace(kv.first, std::move(kv.second));
      }
    }
    return result;
  }

  std::unordered_map<uint64_t, float> Distances(
      const std::string& query_vec, const std::vector<uint64_t>& ids, Metrics* metrics) {
    std::unordered_map<uint64_t, float> result;
    auto groups = GroupIds(ids);
    if (cfg_.distance_mode == "compute") {
      for (const auto& [owner_chunk, owner_ids] : groups) {
        if (metrics) {
          metrics->remote_distance_calls++;
          metrics->remote_candidates_scored += owner_ids.size();
          metrics->distance_batches++;
          metrics->max_candidates_per_distance_batch = std::max<uint64_t>(
              metrics->max_candidates_per_distance_batch, owner_ids.size());
          RecordDistanceBatch(
              metrics, owner_chunk.first, owner_chunk.second, owner_ids.size());
        }
        const double batch_t0 = now_sec();
        std::unordered_map<uint64_t, std::string> vectors;
        const double fetch_t0 = now_sec();
        FetchVectorBatch(owner_chunk.first, owner_chunk.second, owner_ids, &vectors, metrics);
        if (metrics) {
          metrics->distance_fetch_rpc_seconds += now_sec() - fetch_t0;
        }
        const double compute_t0 = now_sec();
        for (const auto& kv : vectors) {
          result.emplace(
              kv.first,
              compute_distance_local(query_vec, kv.second, cfg_.dim, cfg_.vector_kind, cfg_.metric));
        }
        if (metrics) {
          metrics->distance_local_compute_seconds += now_sec() - compute_t0;
          metrics->remote_distance_seconds += now_sec() - batch_t0;
        }
      }
      return result;
    }
    for (const auto& [owner_chunk, owner_ids] : groups) {
      DistanceBatchRequest req;
      req.query_vector = query_vec;
      req.dim = cfg_.dim;
      req.vector_kind = cfg_.vector_kind;
      req.metric = cfg_.metric;
      req.global_ids = owner_ids;
      ceph::bufferlist in = encode_cls_request(
          Opcode::kDistanceToLocalBatch,
          req,
          0,
          0,
          placement_.placement_epoch()), out;
      metrics->remote_distance_calls++;
      metrics->remote_candidates_scored += owner_ids.size();
      metrics->distance_batches++;
      metrics->logical_distance_query_bytes += query_vec.size();
      metrics->max_candidates_per_distance_batch = std::max<uint64_t>(
          metrics->max_candidates_per_distance_batch, owner_ids.size());
      RecordDistanceBatch(
          metrics, owner_chunk.first, owner_chunk.second, owner_ids.size());
      if (cfg_.distance_split_probe && metrics &&
          ShouldSampleNoop(owner_chunk.first, owner_chunk.second)) {
        const TimedNoopSample noop_sample =
            TimedNoop(owner_chunk.first, owner_chunk.second, metrics);
        metrics->remote_noop_calls++;
        metrics->distance_noop_roundtrip_seconds += noop_sample.roundtrip_seconds;
        metrics->distance_noop_cls_total_seconds += noop_sample.cls_total_seconds;
        metrics->distance_network_roundtrip_est_seconds +=
            noop_sample.baseline_non_cls_seconds;
        metrics->distance_osd_queue_est_seconds += noop_sample.queue_est_seconds;
      }
      const double t0 = now_sec();
      int r = Exec(
          owner_ioctxs_[owner_chunk.first], ghnsw::OwnerDataOid(owner_chunk.second),
          "distance_to_local_batch", in, &out, metrics);
      RecordDataTarget(metrics, owner_chunk.first, owner_chunk.second);
      metrics->remote_distance_seconds += now_sec() - t0;
      if (r < 0) {
        throw CephOperationError("distance_to_local_batch", r);
      }
      DistanceBatchReply reply;
      decode_cls_reply_or_die(
          out, Opcode::kDistanceToLocalBatch, &reply, "DistanceBatchReply");
      metrics->distance_cls_total_seconds += reply.cls_total_seconds;
      metrics->distance_vector_ref_seconds += reply.vector_ref_seconds;
      metrics->distance_payload_read_seconds += reply.payload_read_seconds;
      metrics->distance_compute_seconds += reply.compute_seconds;
      for (const auto& kv : reply.distances) {
        result.emplace(kv.first, kv.second);
      }
    }
    return result;
  }

  void SetAdjacency(
      const std::vector<AdjacencyBlob>& entries,
      Metrics* metrics = nullptr,
      uint64_t update_id = 0) {
    if (cfg_.distance_mode == "compute") {
      RawSetAdjacency(entries, metrics);
      return;
    }
    const double t0 = now_sec();
    auto groups = GroupAdj(entries);
    for (const auto& [owner_chunk, owner_entries] : groups) {
      SetAdjacencyBatchRequest req;
      req.entries = owner_entries;
      ceph::bufferlist in = encode_cls_request(
          Opcode::kSetAdjacencyBatch,
          req,
          update_id,
          0,
          placement_.placement_epoch()), out;
      int r = Exec(
          owner_ioctxs_[owner_chunk.first], ghnsw::OwnerDataOid(owner_chunk.second),
          "set_adjacency_batch", in, &out, metrics);
      RecordDataTarget(metrics, owner_chunk.first, owner_chunk.second);
      if (r < 0) {
        throw CephOperationError("set_adjacency_batch", r);
      }
    }
    if (metrics) {
      metrics->set_new_adjacency_seconds += now_sec() - t0;
    }
  }

  void ApplyPatches(
      const std::vector<AdjacencyBlob>& entries, uint64_t update_id, Metrics* metrics) {
    if (cfg_.distance_mode == "compute") {
      RawApplyPatches(entries, update_id, metrics);
      return;
    }
    auto groups = GroupAdj(entries);
    for (const auto& [owner_chunk, owner_entries] : groups) {
      EdgePatchBatchRequest req;
      req.update_id = update_id;
      req.max_neighbors = cfg_.M;
      req.entries = owner_entries;
      ceph::bufferlist in = encode_cls_request(
          Opcode::kApplyEdgePatchBatch,
          req,
          update_id,
          0,
          placement_.placement_epoch()), out;
      metrics->remote_patch_calls++;
      metrics->total_patched_nodes += owner_entries.size();
      const double t0 = now_sec();
      int r = Exec(
          owner_ioctxs_[owner_chunk.first], ghnsw::OwnerDataOid(owner_chunk.second),
          "apply_edge_patch_batch", in, &out, metrics);
      RecordDataTarget(metrics, owner_chunk.first, owner_chunk.second);
      metrics->adjacency_patch_seconds += now_sec() - t0;
      if (r < 0) {
        throw CephOperationError("apply_edge_patch_batch", r);
      }
    }
  }

  void MarkStale(uint64_t global_id, Metrics* metrics, uint64_t update_id = 0) {
    if (cfg_.distance_mode == "compute") {
      RawMarkStale(global_id, metrics);
      return;
    }
    const double t0 = now_sec();
    MarkNodeStaleRequest req;
    req.global_id = global_id;
    const auto location = placement_.Resolve(global_id);
    const uint32_t owner = location.shard_id;
    const uint64_t chunk = location.chunk_id;
    ceph::bufferlist in = encode_cls_request(
        Opcode::kMarkNodeStale, req, update_id, 0, location.placement_epoch), out;
    int r = Exec(
        owner_ioctxs_[owner], ghnsw::OwnerDataOid(chunk),
        "mark_node_stale", in, &out, metrics);
    RecordDataTarget(metrics, owner, chunk);
    if (r < 0) {
      throw CephOperationError("mark_node_stale", r);
    }
    if (metrics) {
      metrics->mark_stale_seconds += now_sec() - t0;
    }
  }

  GlobalMeta GetMeta(Metrics* metrics = nullptr) {
    if (cfg_.distance_mode == "compute") {
      const double t0 = now_sec();
      GlobalMeta meta = RawGetMeta(metrics);
      if (metrics) {
        metrics->meta_read_seconds += now_sec() - t0;
      }
      return meta;
    }
    const double t0 = now_sec();
    ceph::bufferlist out;
    ceph::bufferlist in =
        encode_cls_request(Opcode::kGetGlobalMeta, EmptyRequest{});
    int r = Exec(meta_ioctx_, cfg_.meta_oid, "get_global_meta", in, &out, metrics);
    if (r < 0) {
      throw CephOperationError("get_global_meta", r);
    }
    GetGlobalMetaReply reply;
    decode_cls_reply_or_die(
        out, Opcode::kGetGlobalMeta, &reply, "GetGlobalMetaReply");
    if (metrics) {
      metrics->meta_read_seconds += now_sec() - t0;
    }
    return reply.meta;
  }

  ReserveInsertReply ReserveInsert(uint64_t update_id, Metrics* metrics) {
    if (cfg_.distance_mode == "compute") {
      return RawReserveInsert(update_id, metrics);
    }
    ReserveInsertRequest req;
    req.update_id = update_id;
    ceph::bufferlist in = encode_cls_request(
        Opcode::kReserveInsertId,
        req,
        update_id,
        0,
        placement_.placement_epoch()), out;
    const double t0 = now_sec();
    if (metrics) {
      metrics->global_meta_cas_calls++;
    }
    int r = Exec(meta_ioctx_, cfg_.meta_oid, "reserve_insert_id", in, &out, metrics);
    if (metrics) {
      metrics->global_meta_update_seconds += now_sec() - t0;
    }
    if (r < 0) {
      throw CephOperationError("reserve_insert_id", r);
    }
    std::set<std::string> keys{ghnsw::ReservationKey(update_id)};
    std::map<std::string, ceph::bufferlist> values;
    for (uint32_t attempt = 0; attempt <= cfg_.osd_op_retry_limit; ++attempt) {
      values.clear();
      r = meta_ioctx_.omap_get_vals_by_keys(cfg_.meta_oid, keys, &values);
      if (r != -ETIMEDOUT && r != -ETIME) {
        break;
      }
      if (attempt == cfg_.osd_op_retry_limit) {
        break;
      }
      if (metrics) {
        metrics->rados_exec_retries++;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50 * (attempt + 1)));
    }
    if (r < 0) {
      throw CephOperationError("read_insert_reservation", r);
    }
    auto value = values.find(ghnsw::ReservationKey(update_id));
    if (value == values.end()) {
      throw CephOperationError("read_insert_reservation", -ENOENT);
    }
    ReserveInsertReply reply;
    decode_or_die(value->second, &reply, "ReserveInsertReply");
    return reply;
  }

  void FinalizeInsert(
      uint64_t update_id, uint64_t global_id, uint32_t level, Metrics* metrics) {
    if (cfg_.distance_mode == "compute") {
      RawFinalizeInsert(update_id, global_id, level, metrics);
      return;
    }
    FinalizeInsertRequest req;
    req.update_id = update_id;
    req.global_id = global_id;
    req.level = level;
    ceph::bufferlist in = encode_cls_request(
        Opcode::kFinalizeInsert,
        req,
        update_id,
        0,
        placement_.placement_epoch()), out;
    const double t0 = now_sec();
    if (metrics) {
      metrics->global_meta_cas_calls++;
    }
    int r = Exec(meta_ioctx_, cfg_.meta_oid, "finalize_insert", in, &out, metrics);
    if (metrics) {
      metrics->global_meta_update_seconds += now_sec() - t0;
    }
    if (r < 0) {
      throw CephOperationError("finalize_insert", r);
    }
  }

  bool CasMeta(uint64_t expected_version, const GlobalMeta& meta, Metrics* metrics = nullptr) {
    if (cfg_.distance_mode == "compute") {
      return RawCasMeta(expected_version, meta, metrics);
    }
    CasGlobalMetaRequest req;
    req.expected_version = expected_version;
    req.meta = meta;
    ceph::bufferlist in = encode_cls_request(
        Opcode::kCasGlobalMeta,
        req,
        0,
        expected_version,
        placement_.placement_epoch()), out;
    const double t0 = now_sec();
    if (metrics) {
      metrics->global_meta_cas_calls++;
    }
    int r = Exec(meta_ioctx_, cfg_.meta_oid, "cas_global_meta", in, &out, metrics);
    if (metrics) {
      metrics->global_meta_update_seconds += now_sec() - t0;
    }
    if (r == -EAGAIN) {
      return false;
    }
    if (r < 0) {
      throw CephOperationError("cas_global_meta", r);
    }
    return true;
  }

  void InitMetaIfNeeded() {
    GlobalMeta meta = GetMeta();
    if (meta.version != 0 || meta.cur_element_count != 0 || meta.next_global_id != 0 ||
        meta.dim != 128 || meta.M != 8 || meta.ef != 32 ||
        meta.vector_kind != ghnsw::kVectorKindU8 || meta.metric != ghnsw::kMetricL2) {
      return;
    }
    meta.dim = cfg_.dim;
    meta.M = cfg_.M;
    meta.ef = cfg_.ef;
    meta.vector_kind = cfg_.vector_kind;
    meta.metric = cfg_.metric;
    if (!CasMeta(0, meta)) {
      throw std::runtime_error("init meta CAS failed");
    }
  }

 private:
  void RecordRaw(
      Metrics* metrics,
      const std::string& operation,
      uint64_t request_bytes,
      uint64_t reply_bytes,
      double roundtrip_seconds) const {
    if (!metrics) {
      return;
    }
    metrics->total_raw_rados_calls++;
    metrics->raw_rados_request_bytes += request_bytes;
    metrics->raw_rados_reply_bytes += reply_bytes;
    auto& values = metrics->raw_operation_metrics[operation];
    values.calls++;
    values.request_bytes += request_bytes;
    values.reply_bytes += reply_bytes;
    values.roundtrip_seconds += roundtrip_seconds;
  }

  int RawOmapGet(
      librados::IoCtx& ioctx,
      const std::string& oid,
      const std::set<std::string>& keys,
      std::map<std::string, ceph::bufferlist>* values,
      Metrics* metrics,
      const std::string& operation) {
    uint64_t request_bytes = 0;
    for (const auto& key : keys) {
      request_bytes += key.size();
    }
    int r = 0;
    for (uint32_t attempt = 0; attempt <= cfg_.osd_op_retry_limit; ++attempt) {
      values->clear();
      const double t0 = now_sec();
      r = ioctx.omap_get_vals_by_keys(oid, keys, values);
      uint64_t reply_bytes = 0;
      for (const auto& [key, value] : *values) {
        reply_bytes += key.size() + value.length();
      }
      RecordRaw(
          metrics, operation, request_bytes, reply_bytes, now_sec() - t0);
      if (r != -ETIMEDOUT && r != -ETIME) {
        return r;
      }
      if (attempt == cfg_.osd_op_retry_limit) {
        break;
      }
      if (metrics) {
        metrics->rados_exec_retries++;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50 * (attempt + 1)));
    }
    return r;
  }

  int RawOmapSet(
      librados::IoCtx& ioctx,
      const std::string& oid,
      const std::map<std::string, ceph::bufferlist>& values,
      Metrics* metrics,
      const std::string& operation) {
    uint64_t request_bytes = 0;
    for (const auto& [key, value] : values) {
      request_bytes += key.size() + value.length();
    }
    int r = 0;
    for (uint32_t attempt = 0; attempt <= cfg_.osd_op_retry_limit; ++attempt) {
      const double t0 = now_sec();
      r = ioctx.omap_set(oid, values);
      RecordRaw(metrics, operation, request_bytes, 0, now_sec() - t0);
      if (r != -ETIMEDOUT && r != -ETIME) {
        return r;
      }
      if (attempt == cfg_.osd_op_retry_limit) {
        break;
      }
      if (metrics) {
        metrics->rados_exec_retries++;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50 * (attempt + 1)));
    }
    return r;
  }

  int RawStat(
      librados::IoCtx& ioctx,
      const std::string& oid,
      uint64_t* size,
      time_t* modified,
      Metrics* metrics,
      const std::string& operation) {
    int r = 0;
    for (uint32_t attempt = 0; attempt <= cfg_.osd_op_retry_limit; ++attempt) {
      const double t0 = now_sec();
      r = ioctx.stat(oid, size, modified);
      RecordRaw(
          metrics,
          operation,
          0,
          r == 0 ? sizeof(*size) + sizeof(*modified) : 0,
          now_sec() - t0);
      if (r != -ETIMEDOUT && r != -ETIME) {
        return r;
      }
      if (attempt == cfg_.osd_op_retry_limit) {
        break;
      }
      if (metrics) {
        metrics->rados_exec_retries++;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50 * (attempt + 1)));
    }
    return r;
  }

  GlobalMeta RawGetMeta(Metrics* metrics) {
    std::map<std::string, ceph::bufferlist> values;
    const int r = RawOmapGet(
        meta_ioctx_,
        cfg_.meta_oid,
        global_meta_keys(),
        &values,
        metrics,
        "raw_get_global_meta");
    if (r < 0) {
      throw CephOperationError("raw_get_global_meta", r);
    }
    if (values.find(kMetaVersion) == values.end()) {
      return GlobalMeta{};
    }
    auto required = [&](const char* key) -> const ceph::bufferlist& {
      auto it = values.find(key);
      if (it == values.end()) {
        throw ProtocolError(std::string("missing global meta key: ") + key);
      }
      return it->second;
    };
    GlobalMeta meta;
    meta.enterpoint = decode_scalar_or_die<uint64_t>(required(kMetaEnterPoint), kMetaEnterPoint);
    meta.max_level = decode_scalar_or_die<uint32_t>(required(kMetaMaxLevel), kMetaMaxLevel);
    meta.cur_element_count =
        decode_scalar_or_die<uint64_t>(required(kMetaCurCount), kMetaCurCount);
    meta.next_global_id =
        decode_scalar_or_die<uint64_t>(required(kMetaNextGlobal), kMetaNextGlobal);
    meta.version = decode_scalar_or_die<uint64_t>(required(kMetaVersion), kMetaVersion);
    meta.M = decode_scalar_or_die<uint32_t>(required(kMetaM), kMetaM);
    meta.ef = decode_scalar_or_die<uint32_t>(required(kMetaEf), kMetaEf);
    meta.dim = decode_scalar_or_die<uint32_t>(required(kMetaDim), kMetaDim);
    auto vector_kind = values.find(kMetaVectorKind);
    meta.vector_kind = vector_kind == values.end()
                           ? ghnsw::kVectorKindU8
                           : decode_scalar_or_die<uint32_t>(
                                 vector_kind->second, kMetaVectorKind);
    auto metric = values.find(kMetaMetric);
    meta.metric = metric == values.end()
                      ? ghnsw::kMetricL2
                      : decode_scalar_or_die<uint32_t>(metric->second, kMetaMetric);
    return meta;
  }

  void RawFetchVectorBatch(
      uint32_t owner,
      uint64_t chunk,
      const std::vector<uint64_t>& ids,
      std::unordered_map<uint64_t, std::string>* result,
      Metrics* metrics) {
    const double batch_t0 = now_sec();
    const std::string oid = ghnsw::OwnerDataOid(chunk);
    std::set<std::string> keys;
    for (uint64_t id : ids) {
      keys.insert(ghnsw::VecKey(id));
    }
    std::map<std::string, ceph::bufferlist> values;
    int r = RawOmapGet(
        owner_ioctxs_[owner],
        oid,
        keys,
        &values,
        metrics,
        "raw_get_vector_refs");
    if (r < 0) {
      throw CephOperationError("raw_get_vector_refs", r);
    }

    std::vector<std::pair<uint64_t, VectorRef>> refs;
    refs.reserve(ids.size());
    for (uint64_t id : ids) {
      auto value = values.find(ghnsw::VecKey(id));
      if (value == values.end()) {
        continue;
      }
      VectorRef ref;
      decode_or_die(value->second, &ref, "VectorRef");
      refs.emplace_back(id, ref);
    }

    std::vector<ceph::bufferlist> payloads;
    std::vector<int> read_status;
    uint64_t payload_bytes = 0;
    if (!refs.empty()) {
      for (uint32_t attempt = 0; attempt <= cfg_.osd_op_retry_limit; ++attempt) {
        payloads.assign(refs.size(), ceph::bufferlist{});
        read_status.assign(refs.size(), 0);
        librados::ObjectReadOperation op;
        uint64_t request_bytes = 0;
        for (size_t i = 0; i < refs.size(); ++i) {
          op.read(
              static_cast<size_t>(refs[i].second.offset),
              refs[i].second.bytes,
              &payloads[i],
              &read_status[i]);
          request_bytes += sizeof(uint64_t) * 2;
        }
        ceph::bufferlist aggregate;
        const double t0 = now_sec();
        r = owner_ioctxs_[owner].operate(oid, &op, &aggregate);
        const double roundtrip_seconds = now_sec() - t0;
        uint64_t reply_bytes = 0;
        for (const auto& payload : payloads) {
          reply_bytes += payload.length();
        }
        RecordRaw(
            metrics,
            "raw_read_vector_payloads",
            request_bytes,
            reply_bytes,
            roundtrip_seconds);
        if (r != -ETIMEDOUT && r != -ETIME) {
          break;
        }
        if (attempt == cfg_.osd_op_retry_limit) {
          break;
        }
        if (metrics) {
          metrics->rados_exec_retries++;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50 * (attempt + 1)));
      }
      if (r < 0) {
        throw CephOperationError("raw_read_vector_payloads", r);
      }
      for (size_t i = 0; i < refs.size(); ++i) {
        if (read_status[i] < 0) {
          throw CephOperationError("raw_read_vector_payload", read_status[i]);
        }
        result->emplace(
            refs[i].first,
            std::string(payloads[i].c_str(), payloads[i].length()));
        payload_bytes += payloads[i].length();
      }
    }
    RecordDataTarget(metrics, owner, chunk);
    if (metrics) {
      metrics->remote_vector_calls++;
      metrics->remote_vector_seconds += now_sec() - batch_t0;
      metrics->remote_vector_bytes += payload_bytes;
    }
  }

  void RawStoreVector(
      uint64_t global_id,
      uint64_t external_label,
      const std::string& vec,
      uint32_t level,
      Metrics* metrics,
      bool update_label) {
    const double t0 = now_sec();
    const auto location = placement_.Resolve(global_id);
    const uint32_t owner = location.shard_id;
    const uint64_t chunk = location.chunk_id;
    const std::string oid = ghnsw::OwnerDataOid(chunk);
    const std::string key = ghnsw::VecKey(global_id);
    {
      std::lock_guard<std::mutex> lock(
          raw_object_mutex(owner_ioctxs_[owner].get_id(), oid));
      bool stored = false;
      for (uint32_t attempt = 0; attempt <= cfg_.osd_op_retry_limit; ++attempt) {
        std::map<std::string, ceph::bufferlist> existing;
        int r = RawOmapGet(
            owner_ioctxs_[owner],
            oid,
            {key},
            &existing,
            metrics,
            "raw_get_vector_ref_for_store");
        // A base whose per-owner count is exactly divisible by
        // points_per_object inserts its first update into a new chunk object.
        // Native OMAP lookup reports that object as ENOENT; treat it as an
        // empty key set so the compound payload/OMAP write can create it.
        if (r == -ENOENT) {
          existing.clear();
        } else if (r < 0) {
          throw CephOperationError("raw_get_vector_ref_for_store", r);
        }
        auto found = existing.find(key);
        if (found != existing.end()) {
          VectorRef ref;
          decode_or_die(found->second, &ref, "VectorRef");
          const bool matches =
              ref.global_id == global_id && ref.external_label == external_label &&
              ref.bytes == vec.size() && ref.dim == cfg_.dim &&
              ref.vector_kind == cfg_.vector_kind &&
              ref.flags == ghnsw::kVectorFlagActive && ref.level == level;
          if (!matches) {
            throw CephOperationError("raw_store_vector", -EEXIST);
          }
          stored = true;
          break;
        }

        uint64_t size = 0;
        time_t modified = 0;
        r = RawStat(
            owner_ioctxs_[owner],
            oid,
            &size,
            &modified,
            metrics,
            "raw_stat_vector_object");
        if (r == -ENOENT) {
          size = 0;
        } else if (r < 0) {
          throw CephOperationError("raw_stat_vector_object", r);
        }

        VectorRef ref;
        ref.global_id = global_id;
        ref.external_label = external_label;
        ref.offset = size;
        ref.bytes = static_cast<uint32_t>(vec.size());
        ref.dim = cfg_.dim;
        ref.vector_kind = cfg_.vector_kind;
        ref.flags = ghnsw::kVectorFlagActive;
        ref.level = level;
        ceph::bufferlist payload;
        payload.append(vec);
        std::map<std::string, ceph::bufferlist> ref_value{{key, encode_msg(ref)}};
        librados::ObjectWriteOperation op;
        op.write(size, payload);
        op.omap_set(ref_value);
        const double write_t0 = now_sec();
        r = owner_ioctxs_[owner].operate(oid, &op);
        RecordRaw(
            metrics,
            "raw_store_vector",
            payload.length() + key.size() + ref_value.begin()->second.length(),
            0,
            now_sec() - write_t0);
        if (r == 0) {
          stored = true;
          break;
        }
        if (r != -ETIMEDOUT && r != -ETIME) {
          throw CephOperationError("raw_store_vector", r);
        }
        if (attempt == cfg_.osd_op_retry_limit) {
          throw CephOperationError("raw_store_vector", r);
        }
        if (metrics) {
          metrics->rados_exec_retries++;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50 * (attempt + 1)));
      }
      if (!stored) {
        throw CephOperationError("raw_store_vector", -EIO);
      }
    }
    RecordDataTarget(metrics, owner, chunk);
    if (update_label) {
      RawUpdateLabels({{external_label, global_id}}, metrics);
    }
    if (metrics) {
      metrics->store_vector_seconds += now_sec() - t0;
    }
  }

  void RawUpdateLabels(
      const std::vector<std::pair<uint64_t, uint64_t>>& labels,
      Metrics* metrics) {
    std::map<uint32_t, std::map<std::string, ceph::bufferlist>> groups;
    for (const auto& [label, global_id] : labels) {
      groups[placement_.ResolveLabelShard(label)].emplace(
          ghnsw::LabelKey(label), encode_scalar(global_id));
    }
    for (const auto& [owner, values] : groups) {
      const int r = RawOmapSet(
          owner_ioctxs_[owner],
          ghnsw::OwnerMetaOid(),
          values,
          metrics,
          "raw_update_labels");
      if (r < 0) {
        throw CephOperationError("raw_update_labels", r);
      }
    }
  }

  bool RawCasLabel(
      uint64_t external_label,
      bool expect_missing,
      uint64_t expected_global_id,
      uint64_t replacement_global_id,
      Metrics* metrics) {
    const uint32_t owner = placement_.ResolveLabelShard(external_label);
    const std::string oid = ghnsw::OwnerMetaOid();
    const std::string key = ghnsw::LabelKey(external_label);
    std::lock_guard<std::mutex> lock(
        raw_object_mutex(owner_ioctxs_[owner].get_id(), oid));
    if (metrics) {
      metrics->label_cas_calls++;
    }
    std::map<std::string, ceph::bufferlist> values;
    int r = RawOmapGet(
        owner_ioctxs_[owner],
        oid,
        {key},
        &values,
        metrics,
        "raw_get_label_for_cas");
    if (r < 0) {
      throw CephOperationError("raw_get_label_for_cas", r);
    }
    auto current_value = values.find(key);
    if (current_value != values.end()) {
      const uint64_t current =
          decode_scalar_or_die<uint64_t>(current_value->second, "label target");
      if (current == replacement_global_id) {
        return true;
      }
      if (expect_missing || current != expected_global_id) {
        return false;
      }
    } else if (!expect_missing) {
      return false;
    }
    r = RawOmapSet(
        owner_ioctxs_[owner],
        oid,
        {{key, encode_scalar(replacement_global_id)}},
        metrics,
        "raw_cas_label");
    if (r < 0) {
      throw CephOperationError("raw_cas_label", r);
    }
    return true;
  }

  std::unordered_map<uint64_t, uint64_t> RawLookupLabels(
      const std::vector<uint64_t>& labels,
      Metrics* metrics) {
    std::unordered_map<uint64_t, uint64_t> result;
    std::map<uint32_t, std::vector<uint64_t>> groups;
    for (uint64_t label : labels) {
      groups[placement_.ResolveLabelShard(label)].push_back(label);
    }
    for (const auto& [owner, owner_labels] : groups) {
      std::set<std::string> keys;
      for (uint64_t label : owner_labels) {
        keys.insert(ghnsw::LabelKey(label));
      }
      std::map<std::string, ceph::bufferlist> values;
      const int r = RawOmapGet(
          owner_ioctxs_[owner],
          ghnsw::OwnerMetaOid(),
          keys,
          &values,
          metrics,
          "raw_lookup_labels");
      if (r < 0) {
        throw CephOperationError("raw_lookup_labels", r);
      }
      for (uint64_t label : owner_labels) {
        auto value = values.find(ghnsw::LabelKey(label));
        if (value != values.end()) {
          result.emplace(
              label,
              decode_scalar_or_die<uint64_t>(value->second, "label target"));
        }
      }
    }
    return result;
  }

  std::unordered_map<uint64_t, AdjacencyBlob> RawGetAdjacency(
      const std::vector<uint64_t>& ids,
      Metrics* metrics) {
    std::unordered_map<uint64_t, AdjacencyBlob> result;
    auto groups = GroupIds(ids);
    for (const auto& [owner_chunk, owner_ids] : groups) {
      const uint32_t owner = owner_chunk.first;
      const uint64_t chunk = owner_chunk.second;
      std::set<std::string> keys;
      for (uint64_t id : owner_ids) {
        keys.insert(ghnsw::NodeKey(id));
      }
      std::map<std::string, ceph::bufferlist> values;
      const int r = RawOmapGet(
          owner_ioctxs_[owner],
          ghnsw::OwnerDataOid(chunk),
          keys,
          &values,
          metrics,
          "raw_get_adjacency");
      if (metrics) {
        metrics->remote_adj_calls++;
        metrics->remote_adj_nodes += owner_ids.size();
      }
      RecordDataTarget(metrics, owner, chunk);
      if (r < 0) {
        throw CephOperationError("raw_get_adjacency", r);
      }
      for (uint64_t id : owner_ids) {
        auto value = values.find(ghnsw::NodeKey(id));
        if (value == values.end()) {
          continue;
        }
        AdjacencyBlob adjacency;
        decode_or_die(value->second, &adjacency, "AdjacencyBlob");
        result.emplace(id, std::move(adjacency));
      }
    }
    return result;
  }

  void RawSetAdjacency(
      const std::vector<AdjacencyBlob>& entries,
      Metrics* metrics) {
    const double t0 = now_sec();
    auto groups = GroupAdj(entries);
    for (const auto& [owner_chunk, owner_entries] : groups) {
      const uint32_t owner = owner_chunk.first;
      const uint64_t chunk = owner_chunk.second;
      const std::string oid = ghnsw::OwnerDataOid(chunk);
      std::lock_guard<std::mutex> lock(
          raw_object_mutex(owner_ioctxs_[owner].get_id(), oid));
      std::set<std::string> keys;
      for (const auto& entry : owner_entries) {
        keys.insert(ghnsw::NodeKey(entry.global_id));
      }
      std::map<std::string, ceph::bufferlist> existing;
      int r = RawOmapGet(
          owner_ioctxs_[owner],
          oid,
          keys,
          &existing,
          metrics,
          "raw_get_adjacency_for_create");
      if (r < 0) {
        throw CephOperationError("raw_get_adjacency_for_create", r);
      }
      std::map<std::string, ceph::bufferlist> values;
      for (const auto& entry : owner_entries) {
        const std::string key = ghnsw::NodeKey(entry.global_id);
        if (existing.find(key) == existing.end()) {
          values.emplace(key, encode_msg(entry));
        }
      }
      if (!values.empty()) {
        r = RawOmapSet(
            owner_ioctxs_[owner],
            oid,
            values,
            metrics,
            "raw_set_adjacency");
        if (r < 0) {
          throw CephOperationError("raw_set_adjacency", r);
        }
      }
      RecordDataTarget(metrics, owner, chunk);
    }
    if (metrics) {
      metrics->set_new_adjacency_seconds += now_sec() - t0;
    }
  }

  void RawApplyPatches(
      const std::vector<AdjacencyBlob>& entries,
      uint64_t update_id,
      Metrics* metrics) {
    auto groups = GroupAdj(entries);
    for (const auto& [owner_chunk, owner_entries] : groups) {
      const uint32_t owner = owner_chunk.first;
      const uint64_t chunk = owner_chunk.second;
      const std::string oid = ghnsw::OwnerDataOid(chunk);
      const double t0 = now_sec();
      std::lock_guard<std::mutex> lock(
          raw_object_mutex(owner_ioctxs_[owner].get_id(), oid));
      std::map<uint64_t, AdjacencyBlob> patches;
      for (const auto& patch : owner_entries) {
        auto& merged = patches[patch.global_id];
        merged.global_id = patch.global_id;
        if (merged.neighbors.size() < patch.neighbors.size()) {
          merged.neighbors.resize(patch.neighbors.size());
        }
        for (size_t level = 0; level < patch.neighbors.size(); ++level) {
          for (uint64_t id : patch.neighbors[level]) {
            auto& neighbors = merged.neighbors[level];
            if (std::find(neighbors.begin(), neighbors.end(), id) == neighbors.end()) {
              neighbors.push_back(id);
            }
          }
        }
        merged.level_count = static_cast<uint32_t>(merged.neighbors.size());
      }
      std::set<std::string> keys{ghnsw::PatchKey(update_id)};
      for (const auto& [global_id, _] : patches) {
        keys.insert(ghnsw::NodeKey(global_id));
      }
      std::map<std::string, ceph::bufferlist> existing;
      int r = RawOmapGet(
          owner_ioctxs_[owner],
          oid,
          keys,
          &existing,
          metrics,
          "raw_get_adjacency_for_patch");
      if (r < 0) {
        throw CephOperationError("raw_get_adjacency_for_patch", r);
      }
      if (existing.find(ghnsw::PatchKey(update_id)) == existing.end()) {
        std::map<std::string, ceph::bufferlist> values;
        for (const auto& [global_id, patch] : patches) {
          AdjacencyBlob merged = patch;
          auto current_value = existing.find(ghnsw::NodeKey(global_id));
          if (current_value != existing.end()) {
            decode_or_die(current_value->second, &merged, "AdjacencyBlob");
            if (merged.neighbors.size() < patch.neighbors.size()) {
              merged.neighbors.resize(patch.neighbors.size());
            }
            for (size_t level = 0; level < patch.neighbors.size(); ++level) {
              auto& neighbors = merged.neighbors[level];
              for (uint64_t id : patch.neighbors[level]) {
                if (std::find(neighbors.begin(), neighbors.end(), id) == neighbors.end()) {
                  neighbors.push_back(id);
                }
              }
              const size_t max_neighbors =
                  level == 0 ? static_cast<size_t>(cfg_.M) * 2 : cfg_.M;
              if (neighbors.size() > max_neighbors) {
                neighbors.erase(
                    neighbors.begin(),
                    neighbors.end() - static_cast<std::ptrdiff_t>(max_neighbors));
              }
            }
            merged.level_count = static_cast<uint32_t>(merged.neighbors.size());
          }
          values.emplace(ghnsw::NodeKey(global_id), encode_msg(merged));
        }
        values.emplace(ghnsw::PatchKey(update_id), encode_scalar(update_id));
        r = RawOmapSet(
            owner_ioctxs_[owner],
            oid,
            values,
            metrics,
            "raw_apply_edge_patch_batch");
        if (r < 0) {
          throw CephOperationError("raw_apply_edge_patch_batch", r);
        }
      }
      if (metrics) {
        metrics->remote_patch_calls++;
        metrics->total_patched_nodes += owner_entries.size();
        metrics->adjacency_patch_seconds += now_sec() - t0;
      }
      RecordDataTarget(metrics, owner, chunk);
    }
  }

  void RawMarkStale(uint64_t global_id, Metrics* metrics) {
    const double t0 = now_sec();
    const auto location = placement_.Resolve(global_id);
    const uint32_t owner = location.shard_id;
    const uint64_t chunk = location.chunk_id;
    const std::string oid = ghnsw::OwnerDataOid(chunk);
    const std::string key = ghnsw::VecKey(global_id);
    std::lock_guard<std::mutex> lock(
        raw_object_mutex(owner_ioctxs_[owner].get_id(), oid));
    std::map<std::string, ceph::bufferlist> values;
    int r = RawOmapGet(
        owner_ioctxs_[owner],
        oid,
        {key},
        &values,
        metrics,
        "raw_get_vector_ref_for_stale");
    if (r < 0) {
      throw CephOperationError("raw_get_vector_ref_for_stale", r);
    }
    auto value = values.find(key);
    if (value == values.end()) {
      throw CephOperationError("raw_get_vector_ref_for_stale", -ENOENT);
    }
    VectorRef ref;
    decode_or_die(value->second, &ref, "VectorRef");
    ref.flags = ghnsw::kVectorFlagStale;
    r = RawOmapSet(
        owner_ioctxs_[owner],
        oid,
        {{key, encode_msg(ref)}},
        metrics,
        "raw_mark_node_stale");
    if (r < 0) {
      throw CephOperationError("raw_mark_node_stale", r);
    }
    RecordDataTarget(metrics, owner, chunk);
    if (metrics) {
      metrics->mark_stale_seconds += now_sec() - t0;
    }
  }

  ReserveInsertReply RawReserveInsert(uint64_t update_id, Metrics* metrics) {
    if (update_id == 0) {
      throw CephOperationError("raw_reserve_insert_id", -EINVAL);
    }
    const double t0 = now_sec();
    std::lock_guard<std::mutex> lock(raw_meta_mutex());
    if (metrics) {
      metrics->global_meta_cas_calls++;
    }
    const std::string key = ghnsw::ReservationKey(update_id);
    std::map<std::string, ceph::bufferlist> stored;
    int r = RawOmapGet(
        meta_ioctx_,
        cfg_.meta_oid,
        {key},
        &stored,
        metrics,
        "raw_get_insert_reservation");
    if (r < 0) {
      throw CephOperationError("raw_get_insert_reservation", r);
    }
    auto existing = stored.find(key);
    if (existing != stored.end()) {
      ReserveInsertReply reply;
      decode_or_die(existing->second, &reply, "ReserveInsertReply");
      if (metrics) {
        metrics->global_meta_update_seconds += now_sec() - t0;
      }
      return reply;
    }
    GlobalMeta meta = RawGetMeta(metrics);
    ReserveInsertReply reply;
    reply.global_id = meta.next_global_id;
    reply.search_meta = meta;
    GlobalMeta next = meta;
    next.next_global_id++;
    next.version++;
    auto values = encode_global_meta(next);
    values.emplace(key, encode_msg(reply));
    r = RawOmapSet(
        meta_ioctx_,
        cfg_.meta_oid,
        values,
        metrics,
        "raw_reserve_insert_id");
    if (r < 0) {
      throw CephOperationError("raw_reserve_insert_id", r);
    }
    if (metrics) {
      metrics->global_meta_update_seconds += now_sec() - t0;
    }
    return reply;
  }

  void RawFinalizeInsert(
      uint64_t update_id,
      uint64_t global_id,
      uint32_t level,
      Metrics* metrics) {
    if (update_id == 0) {
      throw CephOperationError("raw_finalize_insert", -EINVAL);
    }
    const double t0 = now_sec();
    std::lock_guard<std::mutex> lock(raw_meta_mutex());
    if (metrics) {
      metrics->global_meta_cas_calls++;
    }
    const std::string key = ghnsw::FinalizedKey(update_id);
    std::map<std::string, ceph::bufferlist> stored;
    int r = RawOmapGet(
        meta_ioctx_,
        cfg_.meta_oid,
        {key},
        &stored,
        metrics,
        "raw_get_finalize_marker");
    if (r < 0) {
      throw CephOperationError("raw_get_finalize_marker", r);
    }
    auto existing = stored.find(key);
    if (existing != stored.end()) {
      const uint64_t finalized_id =
          decode_scalar_or_die<uint64_t>(existing->second, "finalized id");
      if (finalized_id != global_id) {
        throw CephOperationError("raw_finalize_insert", -EEXIST);
      }
      if (metrics) {
        metrics->global_meta_update_seconds += now_sec() - t0;
      }
      return;
    }
    GlobalMeta meta = RawGetMeta(metrics);
    if (global_id >= meta.next_global_id) {
      throw CephOperationError("raw_finalize_insert", -ERANGE);
    }
    if (meta.enterpoint == UINT64_MAX || level > meta.max_level) {
      meta.enterpoint = global_id;
      meta.max_level = level;
    }
    meta.cur_element_count++;
    meta.version++;
    auto values = encode_global_meta(meta);
    values.emplace(key, encode_scalar(global_id));
    r = RawOmapSet(
        meta_ioctx_,
        cfg_.meta_oid,
        values,
        metrics,
        "raw_finalize_insert");
    if (r < 0) {
      throw CephOperationError("raw_finalize_insert", r);
    }
    if (metrics) {
      metrics->global_meta_update_seconds += now_sec() - t0;
    }
  }

  bool RawCasMeta(
      uint64_t expected_version,
      const GlobalMeta& meta,
      Metrics* metrics) {
    const double t0 = now_sec();
    std::lock_guard<std::mutex> lock(raw_meta_mutex());
    if (metrics) {
      metrics->global_meta_cas_calls++;
    }
    const GlobalMeta current = RawGetMeta(metrics);
    if (current.version != expected_version) {
      if (metrics) {
        metrics->global_meta_update_seconds += now_sec() - t0;
      }
      return false;
    }
    const int r = RawOmapSet(
        meta_ioctx_,
        cfg_.meta_oid,
        encode_global_meta(meta),
        metrics,
        "raw_cas_global_meta");
    if (metrics) {
      metrics->global_meta_update_seconds += now_sec() - t0;
    }
    if (r < 0) {
      throw CephOperationError("raw_cas_global_meta", r);
    }
    return true;
  }

  void RecordDataTarget(Metrics* metrics, uint32_t owner, uint64_t chunk) {
    if (!metrics) {
      return;
    }
    metrics->current_owner_shards.insert(owner);
    const std::string oid = ghnsw::OwnerDataOid(chunk);
    metrics->current_data_objects.insert(
        std::to_string(owner_ioctxs_[owner].get_id()) + ":" + oid);
    uint32_t pg = 0;
    if (owner_ioctxs_[owner].get_object_pg_hash_position2(oid, &pg) == 0) {
      metrics->current_data_pgs.insert(
          std::to_string(owner_ioctxs_[owner].get_id()) + ":" + std::to_string(pg));
    }
  }

  void RecordDistanceBatch(
      Metrics* metrics, uint32_t owner, uint64_t chunk, uint64_t candidates) {
    if (!metrics) {
      return;
    }
    metrics->current_distance_objects.insert(
        std::to_string(owner_ioctxs_[owner].get_id()) + ":" +
        ghnsw::OwnerDataOid(chunk));
    metrics->distance_batch_size_histogram[candidates]++;
  }

  void RecordExec(
      Metrics* metrics,
      const std::string& operation,
      const ceph::bufferlist& request,
      const ceph::bufferlist& reply,
      double roundtrip_seconds) const {
    if (!metrics) {
      return;
    }
    metrics->total_cls_exec_calls++;
    metrics->cls_request_bytes += request.length();
    metrics->cls_reply_bytes += reply.length();
    auto& values = metrics->operation_metrics[operation];
    values.calls++;
    values.request_bytes += request.length();
    values.reply_bytes += reply.length();
    values.roundtrip_seconds += roundtrip_seconds;
  }

  std::map<std::pair<uint32_t, uint64_t>, std::vector<uint64_t>> GroupIds(
      const std::vector<uint64_t>& ids) {
    std::map<std::pair<uint32_t, uint64_t>, std::vector<uint64_t>> groups;
    for (uint64_t id : ids) {
      const auto location = placement_.Resolve(id);
      groups[{location.shard_id, location.chunk_id}].push_back(id);
    }
    return groups;
  }

  std::map<std::pair<uint32_t, uint64_t>, std::vector<AdjacencyBlob>> GroupAdj(
      const std::vector<AdjacencyBlob>& entries) {
    std::map<std::pair<uint32_t, uint64_t>, std::vector<AdjacencyBlob>> groups;
    for (const auto& entry : entries) {
      const auto location = placement_.Resolve(entry.global_id);
      groups[{location.shard_id, location.chunk_id}].push_back(entry);
    }
    return groups;
  }

  std::string NoopKey(uint32_t owner, uint64_t chunk) const {
    return std::to_string(owner) + ":" + std::to_string(chunk);
  }

  bool ShouldSampleNoop(uint32_t owner, uint64_t chunk) {
    const std::string key = NoopKey(owner, chunk);
    const double now = now_sec();
    static std::mutex sample_mu;
    static std::unordered_map<std::string, double> last_sample;
    std::lock_guard<std::mutex> lock(sample_mu);
    double& last = last_sample[key];
    if (last > 0.0 && now - last < cfg_.distance_probe_interval_seconds) {
      return false;
    }
    last = now;
    return true;
  }

  TimedNoopSample TimedNoop(uint32_t owner, uint64_t chunk, Metrics* metrics) {
    ceph::bufferlist in = encode_cls_request(
        Opcode::kTimedNoop,
        EmptyRequest{},
        0,
        0,
        placement_.placement_epoch());
    ceph::bufferlist out;
    const double t0 = now_sec();
    int r = Exec(
        owner_ioctxs_[owner], ghnsw::OwnerDataOid(chunk),
        "timed_noop", in, &out, metrics);
    RecordDataTarget(metrics, owner, chunk);
    const double roundtrip_seconds = now_sec() - t0;
    if (r < 0) {
      throw CephOperationError("timed_noop", r);
    }
    TimedNoopReply reply;
    decode_cls_reply_or_die(out, Opcode::kTimedNoop, &reply, "TimedNoopReply");
    TimedNoopSample sample;
    sample.roundtrip_seconds = roundtrip_seconds;
    sample.cls_total_seconds = reply.cls_total_seconds;
    sample.non_cls_seconds = nonnegative(roundtrip_seconds - reply.cls_total_seconds);
    {
      std::lock_guard<std::mutex> lock(noop_floor_mu_);
      double& floor = noop_noncls_floor_[NoopKey(owner, chunk)];
      if (floor <= 0.0 || sample.non_cls_seconds < floor) {
        floor = sample.non_cls_seconds;
      }
      sample.baseline_non_cls_seconds = floor;
    }
    sample.queue_est_seconds =
        nonnegative(sample.non_cls_seconds - sample.baseline_non_cls_seconds);
    return sample;
  }

  Config cfg_;
  ghnsw::PlacementResolver placement_;
  librados::Rados cluster_;
  librados::IoCtx meta_ioctx_;
  std::vector<librados::IoCtx> owner_ioctxs_;
  std::mutex noop_floor_mu_;
  std::unordered_map<std::string, double> noop_noncls_floor_;
};

size_t element_size_for_kind(uint32_t vector_kind) {
  return vector_kind == ghnsw::kVectorKindF32 ? sizeof(float) : sizeof(uint8_t);
}

std::vector<std::string> LoadVectors(
    const std::string& path,
    const std::string& format,
    uint64_t offset,
    uint64_t count,
    uint32_t dim,
    uint32_t vector_kind) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    throw std::runtime_error("failed to open dataset: " + path);
  }
  const size_t elem_size = element_size_for_kind(vector_kind);
  const size_t vec_bytes = static_cast<size_t>(dim) * elem_size;
  if (format == "fbin") {
    uint32_t n = 0;
    uint32_t file_dim = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(uint32_t));
    in.read(reinterpret_cast<char*>(&file_dim), sizeof(uint32_t));
    if (!in) {
      throw std::runtime_error("failed to read fbin header: " + path);
    }
	    if (file_dim != dim) {
	      throw std::runtime_error("fbin dim mismatch: file=" + std::to_string(file_dim) +
	                               " expected=" + std::to_string(dim));
	    }
	    if (offset > n) {
	      throw std::runtime_error("fbin read range exceeds file vector count");
	    }
	    count = std::min<uint64_t>(count, static_cast<uint64_t>(n) - offset);
	    in.seekg(static_cast<std::streamoff>(8 + offset * vec_bytes), std::ios::beg);
  } else if (format == "u8bin") {
    uint32_t n = 0;
    uint32_t file_dim = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(uint32_t));
    in.read(reinterpret_cast<char*>(&file_dim), sizeof(uint32_t));
    if (!in) {
      throw std::runtime_error("failed to read u8bin header: " + path);
    }
    if (file_dim != dim) {
      throw std::runtime_error("u8bin dim mismatch: file=" + std::to_string(file_dim) +
                               " expected=" + std::to_string(dim));
    }
    if (offset > n) {
      throw std::runtime_error("u8bin read range exceeds file vector count");
    }
    count = std::min<uint64_t>(count, static_cast<uint64_t>(n) - offset);
    in.seekg(static_cast<std::streamoff>(8 + offset * vec_bytes), std::ios::beg);
  } else {
    throw std::runtime_error("unsupported input format: " + format);
  }
  std::vector<std::string> out;
  out.reserve(count);
  std::string buf(vec_bytes, '\0');
  for (uint64_t i = 0; i < count; ++i) {
    in.read(buf.data(), static_cast<std::streamsize>(vec_bytes));
    if (!in) {
      throw std::runtime_error("dataset read failed");
    }
    out.push_back(buf);
  }
  return out;
}

std::vector<std::vector<uint32_t>> LoadGroundTruth(
    const std::string& path,
    uint64_t offset,
    uint64_t count,
    uint32_t recall_k) {
  if (path.empty()) {
    return {};
  }
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    throw std::runtime_error("failed to open ground truth: " + path);
  }
  uint32_t query_count = 0;
  uint32_t stored_k = 0;
  in.read(reinterpret_cast<char*>(&query_count), sizeof(query_count));
  in.read(reinterpret_cast<char*>(&stored_k), sizeof(stored_k));
  if (!in || query_count == 0 || stored_k == 0) {
    throw std::runtime_error("invalid ground-truth header: " + path);
  }
  if (recall_k == 0 || recall_k > stored_k) {
    throw std::runtime_error(
        "recall K exceeds ground-truth width: requested=" +
        std::to_string(recall_k) + " stored=" + std::to_string(stored_k));
  }
  if (offset > query_count || count > static_cast<uint64_t>(query_count) - offset) {
    throw std::runtime_error("ground-truth read range exceeds query count");
  }

  const uint64_t row_bytes = static_cast<uint64_t>(stored_k) * sizeof(uint32_t);
  in.seekg(static_cast<std::streamoff>(8 + offset * row_bytes), std::ios::beg);
  if (!in) {
    throw std::runtime_error("failed to seek ground truth: " + path);
  }

  std::vector<std::vector<uint32_t>> rows;
  rows.reserve(count);
  std::vector<uint32_t> stored_row(stored_k);
  for (uint64_t i = 0; i < count; ++i) {
    in.read(
        reinterpret_cast<char*>(stored_row.data()),
        static_cast<std::streamsize>(row_bytes));
    if (!in) {
      throw std::runtime_error("ground-truth row read failed: " + path);
    }
    rows.emplace_back(stored_row.begin(), stored_row.begin() + recall_k);
  }
  return rows;
}

std::vector<float> ToFloatVector(const std::string& raw, uint32_t dim, uint32_t vector_kind) {
  std::vector<float> out(dim);
  if (vector_kind == ghnsw::kVectorKindU8) {
    const uint8_t* src = reinterpret_cast<const uint8_t*>(raw.data());
    for (uint32_t i = 0; i < dim; ++i) {
      out[i] = static_cast<float>(src[i]);
    }
  } else {
    std::memcpy(out.data(), raw.data(), static_cast<size_t>(dim) * sizeof(float));
  }
  return out;
}

uint32_t SampleLevel(std::mt19937_64& rng, uint32_t M) {
  const double inv_log = 1.0 / std::log(static_cast<double>(M));
  std::uniform_real_distribution<double> dist(0.0, 1.0);
  double x = -std::log(dist(rng)) * inv_log;
  return static_cast<uint32_t>(x);
}

std::vector<Neighbor> TopK(std::unordered_map<uint64_t, float>& input, size_t k) {
  std::vector<Neighbor> v;
  v.reserve(input.size());
  for (const auto& kv : input) {
    v.push_back({kv.first, kv.second});
  }
  std::sort(v.begin(), v.end(), [](const Neighbor& a, const Neighbor& b) { return a.dist < b.dist; });
  if (v.size() > k) {
    v.resize(k);
  }
  return v;
}

class StripedLocks {
 public:
  explicit StripedLocks(size_t stripes = 65536) : locks_(stripes) {}

  size_t StripeFor(uint64_t id) const {
    return std::hash<uint64_t>{}(id) % locks_.size();
  }

  std::shared_mutex& MutexForStripe(size_t stripe) {
    return locks_[stripe];
  }

 private:
  std::vector<std::shared_mutex> locks_;
};

class ScopedStripedLocks {
 public:
  ScopedStripedLocks(StripedLocks& locks, const std::vector<uint64_t>& ids) : locks_(locks) {
    std::vector<size_t> stripes;
    stripes.reserve(ids.size());
    for (uint64_t id : ids) {
      stripes.push_back(locks_.StripeFor(id));
    }
    std::sort(stripes.begin(), stripes.end());
    stripes.erase(std::unique(stripes.begin(), stripes.end()), stripes.end());

    guards_.reserve(stripes.size());
    for (size_t stripe : stripes) {
      guards_.emplace_back(locks_.MutexForStripe(stripe));
    }
  }

  ScopedStripedLocks(const ScopedStripedLocks&) = delete;
  ScopedStripedLocks& operator=(const ScopedStripedLocks&) = delete;

 private:
  StripedLocks& locks_;
  std::vector<std::unique_lock<std::shared_mutex>> guards_;
};

class ScopedStripedSharedLocks {
 public:
  ScopedStripedSharedLocks(StripedLocks& locks, const std::vector<uint64_t>& ids) : locks_(locks) {
    std::vector<size_t> stripes;
    stripes.reserve(ids.size());
    for (uint64_t id : ids) {
      stripes.push_back(locks_.StripeFor(id));
    }
    std::sort(stripes.begin(), stripes.end());
    stripes.erase(std::unique(stripes.begin(), stripes.end()), stripes.end());

    guards_.reserve(stripes.size());
    for (size_t stripe : stripes) {
      guards_.emplace_back(locks_.MutexForStripe(stripe));
    }
  }

  ScopedStripedSharedLocks(const ScopedStripedSharedLocks&) = delete;
  ScopedStripedSharedLocks& operator=(const ScopedStripedSharedLocks&) = delete;

 private:
  StripedLocks& locks_;
  std::vector<std::shared_lock<std::shared_mutex>> guards_;
};

class Coordinator {
 public:
  explicit Coordinator(const Config& cfg)
      : cfg_(cfg), ceph_(cfg), rng_(cfg.seed), next_update_id_(make_update_id_seed()) {}

  void Run() {
    ceph_.Connect();
    ceph_.InitMetaIfNeeded();
    auto t0 = now_sec();

    if (cfg_.mode == "build") {
      RunBuild();
    } else if (cfg_.mode == "update") {
      RunUpdate();
    } else {
      throw std::runtime_error("unsupported mode");
    }
    metrics_.total_seconds = now_sec() - t0;
    WriteMetrics();
    if (metrics_.failed_updates > 0) {
      throw std::runtime_error(
          "update run completed with " + std::to_string(metrics_.failed_updates) +
          " failed update(s); see metrics output");
    }
  }

 private:
  struct UpdateContext {
    explicit UpdateContext(CephFacade& ceph_ref, uint64_t seed)
        : ceph(ceph_ref), rng(seed) {}

    CephFacade& ceph;
    Metrics metrics;
    std::mt19937_64 rng;
    std::vector<AdjacencyBlob> pending_patches;
    std::vector<double> update_latencies_ms;
  };

  struct ReservedInsert {
    uint64_t global_id = 0;
    GlobalMeta search_meta;
  };

  struct SelectedLevel {
    uint32_t level = 0;
    std::vector<Neighbor> selected;
  };

  void RunBuild() {
    metrics_.mode = "build";
    auto t0 = now_sec();
    auto vectors = LoadVectors(
        cfg_.input, cfg_.input_format, 0, cfg_.num_vectors, cfg_.dim, cfg_.vector_kind);
    metrics_.load_seconds = now_sec() - t0;
    auto g0 = now_sec();
    for (uint64_t i = 0; i < cfg_.num_vectors; ++i) {
      InsertOne(vectors[i], i);
      metrics_.vectors_processed++;
      if ((i + 1) % 1000 == 0) {
        std::cerr << "build inserted " << (i + 1) << "/" << cfg_.num_vectors << std::endl;
        WriteProgress(i + 1);
      }
    }
    metrics_.graph_seconds = now_sec() - g0;
  }

  void RunUpdate() {
    metrics_.mode = "update";
    metrics_.time_limit_seconds = cfg_.time_limit_seconds;
    metrics_.update_parallelism = cfg_.update_parallelism;
    auto t0 = now_sec();
    auto vectors = LoadVectors(
        cfg_.update_input,
        cfg_.update_input_format,
        cfg_.update_offset,
        cfg_.num_updates,
        cfg_.dim,
        cfg_.vector_kind);
    ground_truth_ = LoadGroundTruth(
        cfg_.ground_truth,
        cfg_.update_offset,
        vectors.size(),
        cfg_.recall_k);
    metrics_.load_seconds = now_sec() - t0;
    if (vectors.empty()) {
      throw std::runtime_error("no update vectors loaded");
    }
    // Use the same idempotent reservation/finalization path at every
    // concurrency level. The legacy serial insertion path remains build-only.
    RunUpdateParallel(vectors);
  }

	  void RunUpdateParallel(const std::vector<std::string>& vectors) {
	    const double g0 = now_sec();
	    const double deadline =
	        cfg_.time_limit_seconds > 0 ? (g0 + static_cast<double>(cfg_.time_limit_seconds)) : 0.0;
	    double start_cutoff = deadline;
	    if (cfg_.time_limit_seconds > 0) {
	      // Do not launch a fresh update right at the window boundary. A single
		      // HNSW update can have a long tail because it issues many remote ops.
	      const double grace =
	          std::min(30.0, static_cast<double>(cfg_.time_limit_seconds) * 0.10);
	      start_cutoff = std::max(g0, deadline - grace);
	    }
	    const uint64_t label_span = std::max<uint64_t>(1, cfg_.num_updates);
    std::atomic<uint64_t> next_update{0};
    std::atomic<uint64_t> completed{0};
    std::atomic<bool> stop{false};
    std::atomic<bool> stopped_by_time{false};
    std::exception_ptr first_error;
	    std::mutex error_mu;
	    std::mutex merge_mu;
	    std::mutex progress_mu;
	    std::vector<std::thread> threads;
	    threads.reserve(cfg_.update_parallelism);

	    auto worker = [&](uint32_t tid) {
	      CephFacade worker_ceph(cfg_);
	      try {
	        worker_ceph.Connect();
	      } catch (...) {
	        {
	          std::lock_guard<std::mutex> lock(error_mu);
	          if (!first_error) {
	            first_error = std::current_exception();
	          }
	        }
	        stop.store(true, std::memory_order_relaxed);
	        return;
	      }
	      UpdateContext ctx(worker_ceph, static_cast<uint64_t>(cfg_.seed) + 0x9e3779b97f4a7c15ULL * (tid + 1));
      ctx.metrics.mode = "update";
      ctx.metrics.time_limit_seconds = cfg_.time_limit_seconds;
      ctx.metrics.update_parallelism = cfg_.update_parallelism;
      try {
        while (!stop.load(std::memory_order_relaxed)) {
	          if (cfg_.time_limit_seconds > 0 && now_sec() >= start_cutoff) {
	            stopped_by_time.store(true, std::memory_order_relaxed);
	            stop.store(true, std::memory_order_relaxed);
	            break;
          }

          const uint64_t i = next_update.fetch_add(1, std::memory_order_relaxed);
          if (cfg_.time_limit_seconds == 0 && i >= cfg_.num_updates) {
            break;
          }

	          try {
	            const uint64_t vec_idx = i % vectors.size();
	            const uint64_t label = cfg_.target_start + (i % label_span);
	            begin_update_observation(&ctx.metrics);
	            const double update_t0 = now_sec();
	            {
	              ScopedStripedLocks label_lock(label_locks_, {label});
	              const double lookup_t0 = now_sec();
	              auto labels = ctx.ceph.LookupLabels({label}, &ctx.metrics);
	              ctx.metrics.lookup_old_seconds += now_sec() - lookup_t0;
	              auto it = labels.find(label);
              const uint64_t update_id = NextUpdateId();
              std::vector<uint64_t> recall_candidates;
              const uint64_t new_id = InsertOneParallel(
                  ctx,
                  vectors[vec_idx],
                  label,
                  update_id,
                  ground_truth_.empty() ? nullptr : &recall_candidates);
              if (!ctx.ceph.CasLabel(
                      label, it == labels.end(), it == labels.end() ? 0 : it->second,
                      new_id, &ctx.metrics, update_id)) {
                ctx.metrics.label_cas_conflicts++;
                ctx.ceph.MarkStale(new_id, &ctx.metrics, update_id);
                ctx.metrics.stale_marks++;
                throw CephOperationError("label CAS", -EAGAIN);
              }
              if (it != labels.end()) {
                ctx.ceph.MarkStale(it->second, &ctx.metrics, update_id);
                ctx.metrics.stale_marks++;
              }
              if (!ground_truth_.empty()) {
                RecordRecall(
                    &ctx.metrics,
                    vec_idx,
                    i,
                    recall_candidates,
                    ground_truth_.at(vec_idx));
              }
            }
            finish_update_observation(&ctx.metrics);
            ctx.update_latencies_ms.push_back((now_sec() - update_t0) * 1000.0);
            ctx.metrics.vectors_processed++;

            const uint64_t done = completed.fetch_add(1, std::memory_order_relaxed) + 1;
            if (done % 100 == 0) {
              std::lock_guard<std::mutex> lock(progress_mu);
              if (cfg_.time_limit_seconds > 0) {
                std::cerr << "parallel update inserted " << done << " in "
                          << (now_sec() - g0) << "s / " << cfg_.time_limit_seconds
                          << "s, workers=" << cfg_.update_parallelism << std::endl;
              } else {
                std::cerr << "parallel update inserted " << done << "/"
                          << cfg_.num_updates << ", workers=" << cfg_.update_parallelism
                          << std::endl;
              }
              WriteProgress(done);
            }
          } catch (const std::exception& error) {
            record_update_failure(&ctx.metrics, error);
            finish_update_observation(&ctx.metrics);
          } catch (...) {
            ctx.metrics.failed_updates++;
            ctx.metrics.failed_update_other++;
            ctx.metrics.failure_operations["unknown"]++;
            finish_update_observation(&ctx.metrics);
          }
        }
      } catch (...) {
        {
          std::lock_guard<std::mutex> lock(error_mu);
          if (!first_error) {
            first_error = std::current_exception();
          }
        }
        stop.store(true, std::memory_order_relaxed);
      }

      std::lock_guard<std::mutex> lock(merge_mu);
      merge_update_metrics(&metrics_, ctx.metrics);
      update_latencies_ms_.insert(
          update_latencies_ms_.end(),
          ctx.update_latencies_ms.begin(),
          ctx.update_latencies_ms.end());
    };

    for (uint32_t tid = 0; tid < cfg_.update_parallelism; ++tid) {
      threads.emplace_back(worker, tid);
    }
    for (auto& thread : threads) {
      thread.join();
    }
    if (first_error) {
      std::rethrow_exception(first_error);
    }

    metrics_.graph_seconds = now_sec() - g0;
    metrics_.stopped_by_time_limit = stopped_by_time.load(std::memory_order_relaxed);
    if (metrics_.graph_seconds > 0.0) {
      metrics_.throughput_updates_per_sec =
          static_cast<double>(metrics_.vectors_processed) / metrics_.graph_seconds;
    }
    if (!update_latencies_ms_.empty()) {
      double sum = 0.0;
      for (double v : update_latencies_ms_) {
        sum += v;
      }
      metrics_.avg_update_latency_ms = sum / static_cast<double>(update_latencies_ms_.size());
      metrics_.p50_update_latency_ms = percentile_ms(update_latencies_ms_, 0.50);
      metrics_.p95_update_latency_ms = percentile_ms(update_latencies_ms_, 0.95);
      metrics_.p99_update_latency_ms = percentile_ms(update_latencies_ms_, 0.99);
    }
  }

  uint64_t NextUpdateId() {
    uint64_t id = next_update_id_.fetch_add(1, std::memory_order_relaxed);
    if (id == 0) {
      id = next_update_id_.fetch_add(1, std::memory_order_relaxed);
    }
    return id;
  }

  ReservedInsert ReserveInsertId(UpdateContext& ctx, uint64_t update_id) {
    ReserveInsertReply reply = ctx.ceph.ReserveInsert(update_id, &ctx.metrics);
    return {reply.global_id, reply.search_meta};
  }

  void FinalizeInsertMeta(
      UpdateContext& ctx, uint64_t update_id, uint64_t global_id, uint32_t level) {
    ctx.ceph.FinalizeInsert(update_id, global_id, level, &ctx.metrics);
  }

  float DistanceToOne(UpdateContext& ctx, const std::string& query, uint64_t id) {
    auto distances = ctx.ceph.Distances(query, {id}, &ctx.metrics);
    auto it = distances.find(id);
    if (it == distances.end()) {
      return std::numeric_limits<float>::max();
    }
    return it->second;
  }

  std::unordered_map<uint64_t, float> DistanceToMany(
      UpdateContext& ctx, const std::string& query, const std::vector<uint64_t>& ids) {
    return ctx.ceph.Distances(query, ids, &ctx.metrics);
  }

	  AdjacencyBlob GetAdjNoLock(UpdateContext& ctx, uint64_t id) {
	    auto adjs = ctx.ceph.GetAdjacency({id}, &ctx.metrics);
	    auto it = adjs.find(id);
	    if (it == adjs.end()) {
      AdjacencyBlob empty;
      empty.global_id = id;
      empty.level_count = 0;
      return empty;
    }
    return it->second;
  }

	  std::string GetVector(UpdateContext& ctx, uint64_t id) {
	    auto vecs = ctx.ceph.GetVectors({id}, &ctx.metrics);
	    auto it = vecs.find(id);
    if (it == vecs.end()) {
      throw std::runtime_error("vector not found");
    }
	    return it->second;
	  }

	  AdjacencyBlob GetAdj(UpdateContext& ctx, uint64_t id) {
	    return GetAdjNoLock(ctx, id);
	  }

	  AdjacencyBlob* FindPendingPatch(std::vector<AdjacencyBlob>& patches, uint64_t id) {
	    for (auto& patch : patches) {
	      if (patch.global_id == id) {
	        return &patch;
	      }
	    }
	    return nullptr;
	  }

  uint64_t GreedySearch(
      UpdateContext& ctx, const std::string& query, uint64_t entry, uint32_t level, float* out_dist) {
    uint64_t current = entry;
    float current_dist = DistanceToOne(ctx, query, current);
    while (true) {
      auto adj = GetAdj(ctx, current);
      if (level >= adj.neighbors.size()) {
        break;
      }
      const auto& nbrs = adj.neighbors[level];
      if (nbrs.empty()) {
        break;
      }
      auto dists = DistanceToMany(ctx, query, nbrs);
      bool improved = false;
      for (uint64_t nbr : nbrs) {
        auto it = dists.find(nbr);
        if (it != dists.end() && it->second < current_dist) {
          current = nbr;
          current_dist = it->second;
          improved = true;
        }
      }
      if (!improved) {
        break;
      }
    }
    *out_dist = current_dist;
    return current;
  }

  std::vector<Neighbor> SearchLayer(
      UpdateContext& ctx, const std::string& query, uint64_t entry, uint32_t level, uint32_t ef) {
    struct MinCmp {
      bool operator()(const Neighbor& a, const Neighbor& b) const { return a.dist > b.dist; }
    };
    struct MaxCmp {
      bool operator()(const Neighbor& a, const Neighbor& b) const { return a.dist < b.dist; }
    };

    std::priority_queue<Neighbor, std::vector<Neighbor>, MinCmp> candidates;
    std::priority_queue<Neighbor, std::vector<Neighbor>, MaxCmp> top;
    std::set<uint64_t> visited;

    float entry_dist = DistanceToOne(ctx, query, entry);
    candidates.push({entry, entry_dist});
    top.push({entry, entry_dist});
    visited.insert(entry);

    while (!candidates.empty()) {
      Neighbor cand = candidates.top();
      candidates.pop();
      if (cand.dist > top.top().dist) {
        break;
      }
      ctx.metrics.frontier_windows++;
      ctx.metrics.frontier_nodes_selected++;
      auto adj = GetAdj(ctx, cand.id);
      if (level >= adj.neighbors.size()) {
        continue;
      }
      ctx.metrics.distance_candidates_before_dedupe +=
          adj.neighbors[level].size();
      std::vector<uint64_t> to_score;
      for (uint64_t nbr : adj.neighbors[level]) {
        if (visited.insert(nbr).second) {
          to_score.push_back(nbr);
        }
      }
      if (to_score.empty()) {
        continue;
      }
      ctx.metrics.distance_candidates_after_dedupe += to_score.size();
      auto dists = DistanceToMany(ctx, query, to_score);
      for (uint64_t nbr : to_score) {
        auto it = dists.find(nbr);
        if (it == dists.end()) {
          continue;
        }
        Neighbor nn{nbr, it->second};
        if (top.size() < ef || nn.dist < top.top().dist) {
          candidates.push(nn);
          top.push(nn);
          if (top.size() > ef) {
            top.pop();
          }
        }
      }
    }

    std::vector<Neighbor> result;
    while (!top.empty()) {
      result.push_back(top.top());
      top.pop();
    }
    std::sort(result.begin(), result.end(), [](const Neighbor& a, const Neighbor& b) {
      return a.dist < b.dist;
    });
    return result;
  }

	  void PatchNeighborAtomic(
	      UpdateContext& ctx, uint64_t target, uint32_t level, uint64_t new_id) {
	    AdjacencyBlob* pending = FindPendingPatch(ctx.pending_patches, target);
	    if (!pending) {
	      AdjacencyBlob patch;
	      patch.global_id = target;
	      patch.neighbors.resize(level + 1);
	      patch.level_count = static_cast<uint32_t>(patch.neighbors.size());
	      patch.neighbors[level].push_back(new_id);
	      ctx.pending_patches.push_back(std::move(patch));
	      return;
	    }
	    if (pending->neighbors.size() <= level) {
	      pending->neighbors.resize(level + 1);
	      pending->level_count = static_cast<uint32_t>(pending->neighbors.size());
	    }
	    auto& list = pending->neighbors[level];
	    if (std::find(list.begin(), list.end(), new_id) == list.end()) {
	      list.push_back(new_id);
	    }
	  }

  uint64_t InsertOneParallel(
      UpdateContext& ctx,
      const std::string& vector,
      uint64_t external_label,
      uint64_t update_id,
      std::vector<uint64_t>* recall_candidates) {
    ReservedInsert reserved = ReserveInsertId(ctx, update_id);
    GlobalMeta meta = reserved.search_meta;
    const uint64_t global_id = reserved.global_id;
    const uint32_t level = SampleLevel(ctx.rng, cfg_.M);
    ctx.ceph.StoreVector(
        global_id, external_label, vector, level, &ctx.metrics, false, update_id);

    AdjacencyBlob new_adj;
    new_adj.global_id = global_id;
    new_adj.level_count = level + 1;
    new_adj.neighbors.resize(level + 1);

    if (meta.cur_element_count == 0 || meta.enterpoint == UINT64_MAX) {
      ctx.ceph.SetAdjacency({new_adj}, &ctx.metrics, update_id);
      FinalizeInsertMeta(ctx, update_id, global_id, level);
      return global_id;
    }

    uint64_t entry = meta.enterpoint;
    float entry_dist = 0.0f;
    double search_t0 = now_sec();
    double search_dist_t0 = ctx.metrics.remote_distance_seconds;
    if (meta.max_level > level) {
      for (int l = static_cast<int>(meta.max_level); l > static_cast<int>(level); --l) {
        entry = GreedySearch(ctx, vector, entry, static_cast<uint32_t>(l), &entry_dist);
      }
    } else {
      entry_dist = DistanceToOne(ctx, vector, entry);
    }
    ctx.metrics.graph_search_seconds += now_sec() - search_t0;
    ctx.metrics.graph_search_distance_seconds +=
        ctx.metrics.remote_distance_seconds - search_dist_t0;

	    std::vector<SelectedLevel> selected_levels;
	    for (int l = static_cast<int>(std::min(level, meta.max_level)); l >= 0; --l) {
	      search_t0 = now_sec();
	      search_dist_t0 = ctx.metrics.remote_distance_seconds;
      auto found = SearchLayer(ctx, vector, entry, static_cast<uint32_t>(l), cfg_.ef);
      ctx.metrics.graph_search_seconds += now_sec() - search_t0;
      ctx.metrics.graph_search_distance_seconds +=
          ctx.metrics.remote_distance_seconds - search_dist_t0;
	      if (l == 0 && recall_candidates != nullptr) {
	        recall_candidates->clear();
	        const size_t limit = std::min<size_t>(cfg_.recall_k, found.size());
	        recall_candidates->reserve(limit);
	        for (size_t i = 0; i < limit; ++i) {
	          recall_candidates->push_back(found[i].id);
	        }
	      }
	      auto selected = SelectNeighbors(found, l == 0 ? cfg_.M * 2 : cfg_.M);
	      auto& level_neighbors = new_adj.neighbors[l];
	      for (const auto& nbr : selected) {
	        level_neighbors.push_back(nbr.id);
	        if (ctx.ceph.Resolve(global_id).shard_id !=
	            ctx.ceph.Resolve(nbr.id).shard_id) {
	          ctx.metrics.cross_owner_neighbor_links++;
	        }
	      }
	      ctx.metrics.total_neighbor_links += level_neighbors.size();
	      if (!selected.empty()) {
	        entry = selected.front().id;
	      }
	      selected_levels.push_back({static_cast<uint32_t>(l), std::move(selected)});
	    }

	    std::map<uint64_t, std::vector<uint32_t>> patch_targets;
	    for (const auto& level_selection : selected_levels) {
	      for (const auto& nbr : level_selection.selected) {
	        patch_targets[nbr.id].push_back(level_selection.level);
	      }
	    }

	    ctx.ceph.SetAdjacency({new_adj}, &ctx.metrics, update_id);
	    ctx.pending_patches.clear();
	    const double patch_t0 = now_sec();
	    for (const auto& [target, levels] : patch_targets) {
	      for (uint32_t patch_level : levels) {
	        PatchNeighborAtomic(ctx, target, patch_level, global_id);
	      }
	    }
	    ctx.metrics.patch_prepare_seconds += now_sec() - patch_t0;
	    if (!ctx.pending_patches.empty()) {
	      ctx.ceph.ApplyPatches(ctx.pending_patches, update_id, &ctx.metrics);
	    }
	    ctx.pending_patches.clear();
	    FinalizeInsertMeta(ctx, update_id, global_id, level);
	    return global_id;
	  }

  void RecordRecall(
      Metrics* metrics,
      uint64_t query_index,
      uint64_t update_sequence,
      const std::vector<uint64_t>& candidates,
      const std::vector<uint32_t>& ground_truth) const {
    const size_t denominator = std::min<size_t>(cfg_.recall_k, ground_truth.size());
    if (denominator == 0) {
      return;
    }
    std::unordered_set<uint64_t> candidate_ids;
    const size_t candidate_count = std::min(denominator, candidates.size());
    candidate_ids.reserve(candidate_count);
    for (size_t i = 0; i < candidate_count; ++i) {
      candidate_ids.insert(candidates[i]);
    }
    uint64_t hits = 0;
    for (size_t i = 0; i < denominator; ++i) {
      if (candidate_ids.count(ground_truth[i]) != 0) {
        ++hits;
      }
    }
    metrics->recall_evaluated_updates++;
    metrics->recall_hits += hits;
    metrics->recall_denominator += denominator;
    auto found = metrics->recall_samples.find(query_index);
    if (found == metrics->recall_samples.end() ||
        update_sequence < found->second.update_sequence) {
      metrics->recall_samples[query_index] = {
          update_sequence, hits, static_cast<uint64_t>(denominator)};
    }
  }

  float DistanceToOne(const std::string& query, uint64_t id) {
    auto distances = ceph_.Distances(query, {id}, &metrics_);
    auto it = distances.find(id);
    if (it == distances.end()) {
      return std::numeric_limits<float>::max();
    }
    return it->second;
  }

  std::unordered_map<uint64_t, float> DistanceToMany(const std::string& query, const std::vector<uint64_t>& ids) {
    return ceph_.Distances(query, ids, &metrics_);
  }

  AdjacencyBlob GetAdj(uint64_t id) {
    auto adjs = ceph_.GetAdjacency({id}, &metrics_);
    auto it = adjs.find(id);
    if (it == adjs.end()) {
      AdjacencyBlob empty;
      empty.global_id = id;
      empty.level_count = 0;
      return empty;
    }
    return it->second;
  }

  std::string GetVector(uint64_t id) {
    auto vecs = ceph_.GetVectors({id}, &metrics_);
    auto it = vecs.find(id);
    if (it == vecs.end()) {
      throw std::runtime_error("vector not found");
    }
    return it->second;
  }

  uint64_t GreedySearch(const std::string& query, uint64_t entry, uint32_t level, float* out_dist) {
    uint64_t current = entry;
    float current_dist = DistanceToOne(query, current);
    while (true) {
      auto adj = GetAdj(current);
      if (level >= adj.neighbors.size()) {
        break;
      }
      const auto& nbrs = adj.neighbors[level];
      if (nbrs.empty()) {
        break;
      }
      auto dists = DistanceToMany(query, nbrs);
      bool improved = false;
      for (uint64_t nbr : nbrs) {
        auto it = dists.find(nbr);
        if (it != dists.end() && it->second < current_dist) {
          current = nbr;
          current_dist = it->second;
          improved = true;
        }
      }
      if (!improved) {
        break;
      }
    }
    *out_dist = current_dist;
    return current;
  }

  std::vector<Neighbor> SearchLayer(
      const std::string& query, uint64_t entry, uint32_t level, uint32_t ef) {
    struct MinCmp {
      bool operator()(const Neighbor& a, const Neighbor& b) const { return a.dist > b.dist; }
    };
    struct MaxCmp {
      bool operator()(const Neighbor& a, const Neighbor& b) const { return a.dist < b.dist; }
    };

    std::priority_queue<Neighbor, std::vector<Neighbor>, MinCmp> candidates;
    std::priority_queue<Neighbor, std::vector<Neighbor>, MaxCmp> top;
    std::set<uint64_t> visited;

    float entry_dist = DistanceToOne(query, entry);
    candidates.push({entry, entry_dist});
    top.push({entry, entry_dist});
    visited.insert(entry);

    while (!candidates.empty()) {
      Neighbor cand = candidates.top();
      candidates.pop();
      if (cand.dist > top.top().dist) {
        break;
      }
      metrics_.frontier_windows++;
      metrics_.frontier_nodes_selected++;
      auto adj = GetAdj(cand.id);
      if (level >= adj.neighbors.size()) {
        continue;
      }
      metrics_.distance_candidates_before_dedupe += adj.neighbors[level].size();
      std::vector<uint64_t> to_score;
      for (uint64_t nbr : adj.neighbors[level]) {
        if (visited.insert(nbr).second) {
          to_score.push_back(nbr);
        }
      }
      if (to_score.empty()) {
        continue;
      }
      metrics_.distance_candidates_after_dedupe += to_score.size();
      auto dists = DistanceToMany(query, to_score);
      for (uint64_t nbr : to_score) {
        auto it = dists.find(nbr);
        if (it == dists.end()) {
          continue;
        }
        Neighbor nn{nbr, it->second};
        if (top.size() < ef || nn.dist < top.top().dist) {
          candidates.push(nn);
          top.push(nn);
          if (top.size() > ef) {
            top.pop();
          }
        }
      }
    }

    std::vector<Neighbor> result;
    while (!top.empty()) {
      result.push_back(top.top());
      top.pop();
    }
    std::sort(result.begin(), result.end(), [](const Neighbor& a, const Neighbor& b) { return a.dist < b.dist; });
    return result;
  }

  std::vector<Neighbor> SelectNeighbors(const std::vector<Neighbor>& candidates, size_t max_m) {
    std::vector<Neighbor> out = candidates;
    if (out.size() > max_m) {
      out.resize(max_m);
    }
    return out;
  }

	  void PatchNeighbor(uint64_t target, uint32_t level, uint64_t new_id) {
	    AdjacencyBlob* pending = FindPendingPatch(pending_patches_, target);
	    if (!pending) {
	      AdjacencyBlob patch;
	      patch.global_id = target;
	      patch.neighbors.resize(level + 1);
	      patch.level_count = static_cast<uint32_t>(patch.neighbors.size());
	      patch.neighbors[level].push_back(new_id);
	      pending_patches_.push_back(std::move(patch));
	      return;
	    }
	    if (pending->neighbors.size() <= level) {
	      pending->neighbors.resize(level + 1);
	      pending->level_count = static_cast<uint32_t>(pending->neighbors.size());
	    }
	    auto& list = pending->neighbors[level];
	    if (std::find(list.begin(), list.end(), new_id) == list.end()) {
	      list.push_back(new_id);
	    }
	  }

  uint64_t InsertOne(
      const std::string& vector, uint64_t external_label, bool update_label = true) {
    const uint64_t update_id = NextUpdateId();
    GlobalMeta meta = ceph_.GetMeta(&metrics_);
    const uint64_t global_id = meta.next_global_id;
    const uint32_t level = SampleLevel(rng_, cfg_.M);
    ceph_.StoreVector(
        global_id, external_label, vector, level, &metrics_, false, update_id);

    AdjacencyBlob new_adj;
    new_adj.global_id = global_id;
    new_adj.level_count = level + 1;
    new_adj.neighbors.resize(level + 1);

    if (meta.cur_element_count == 0) {
      ceph_.SetAdjacency({new_adj}, &metrics_, update_id);
      meta.enterpoint = global_id;
      meta.max_level = level;
      meta.cur_element_count = 1;
      meta.next_global_id = 1;
      meta.version += 1;
      if (!ceph_.CasMeta(meta.version - 1, meta, &metrics_)) {
        throw CephOperationError("meta CAS on first insert", -EAGAIN);
      }
      if (update_label) {
        ceph_.UpdateLabels({{external_label, global_id}}, &metrics_, update_id);
      }
      return global_id;
    }

    uint64_t entry = meta.enterpoint;
    float entry_dist = 0.0f;
    double search_t0 = now_sec();
    double search_dist_t0 = metrics_.remote_distance_seconds;
    if (meta.max_level > level) {
      for (int l = static_cast<int>(meta.max_level); l > static_cast<int>(level); --l) {
        entry = GreedySearch(vector, entry, static_cast<uint32_t>(l), &entry_dist);
      }
    } else {
      entry_dist = DistanceToOne(vector, entry);
    }
    metrics_.graph_search_seconds += now_sec() - search_t0;
    metrics_.graph_search_distance_seconds += metrics_.remote_distance_seconds - search_dist_t0;

    pending_patches_.clear();
    for (int l = static_cast<int>(std::min(level, meta.max_level)); l >= 0; --l) {
      search_t0 = now_sec();
      search_dist_t0 = metrics_.remote_distance_seconds;
      auto found = SearchLayer(vector, entry, static_cast<uint32_t>(l), cfg_.ef);
      metrics_.graph_search_seconds += now_sec() - search_t0;
      metrics_.graph_search_distance_seconds += metrics_.remote_distance_seconds - search_dist_t0;
      auto selected = SelectNeighbors(found, l == 0 ? cfg_.M * 2 : cfg_.M);
      auto& level_neighbors = new_adj.neighbors[l];
      for (const auto& nbr : selected) {
        level_neighbors.push_back(nbr.id);
        if (ceph_.Resolve(global_id).shard_id != ceph_.Resolve(nbr.id).shard_id) {
          metrics_.cross_owner_neighbor_links++;
        }
      }
      metrics_.total_neighbor_links += level_neighbors.size();
      for (const auto& nbr : selected) {
        const double patch_t0 = now_sec();
        const double patch_dist_t0 = metrics_.remote_distance_seconds;
        PatchNeighbor(nbr.id, static_cast<uint32_t>(l), global_id);
        metrics_.patch_prepare_seconds += now_sec() - patch_t0;
        metrics_.patch_prepare_distance_seconds += metrics_.remote_distance_seconds - patch_dist_t0;
      }
      if (!selected.empty()) {
        entry = selected.front().id;
      }
    }

    ceph_.SetAdjacency({new_adj}, &metrics_, update_id);
    if (!pending_patches_.empty()) {
      ceph_.ApplyPatches(pending_patches_, update_id, &metrics_);
    }

    if (level > meta.max_level) {
      meta.max_level = level;
      meta.enterpoint = global_id;
    }
    meta.cur_element_count += 1;
    meta.next_global_id += 1;
    meta.version += 1;
    if (!ceph_.CasMeta(meta.version - 1, meta, &metrics_)) {
      throw CephOperationError("meta CAS", -EAGAIN);
    }
    if (update_label) {
      ceph_.UpdateLabels({{external_label, global_id}}, &metrics_, update_id);
    }
    return global_id;
  }

  void WriteMetrics() {
    if (cfg_.metrics_out.empty()) {
      return;
    }
    metrics_.full_graph_search_exclusive_seconds =
        nonnegative(metrics_.graph_search_seconds - metrics_.graph_search_distance_seconds);
    metrics_.adjacency_patch_exclusive_seconds =
        nonnegative(metrics_.patch_prepare_seconds - metrics_.patch_prepare_distance_seconds) +
        metrics_.adjacency_patch_seconds;
    metrics_.distance_cls_accounted_seconds =
        metrics_.distance_vector_ref_seconds +
        metrics_.distance_payload_read_seconds +
        metrics_.distance_compute_seconds;
    metrics_.distance_compute_node_unaccounted_seconds =
        nonnegative(metrics_.remote_distance_seconds -
                    metrics_.distance_fetch_rpc_seconds -
                    metrics_.distance_local_compute_seconds);
    metrics_.distance_cls_unaccounted_seconds =
        nonnegative(metrics_.distance_cls_total_seconds - metrics_.distance_cls_accounted_seconds);
    metrics_.distance_roundtrip_queue_seconds =
        nonnegative(metrics_.remote_distance_seconds - metrics_.distance_cls_total_seconds);
    metrics_.distance_unaccounted_seconds =
        nonnegative(metrics_.remote_distance_seconds - metrics_.distance_cls_accounted_seconds);
    metrics_.accounted_update_seconds =
        metrics_.lookup_old_seconds +
        metrics_.mark_stale_seconds +
        metrics_.full_graph_search_exclusive_seconds +
        metrics_.remote_distance_seconds +
        metrics_.adjacency_patch_exclusive_seconds +
        metrics_.global_meta_update_seconds;
    metrics_.other_update_seconds =
        nonnegative(metrics_.graph_seconds - metrics_.accounted_update_seconds);
    auto pct = [&](double seconds) -> double {
      return metrics_.graph_seconds > 0.0 ? seconds * 100.0 / metrics_.graph_seconds : 0.0;
    };
    auto per_update_ms = [&](double seconds) -> double {
      return metrics_.vectors_processed > 0
                 ? seconds * 1000.0 / static_cast<double>(metrics_.vectors_processed)
                 : 0.0;
    };
    auto per_attempt = [&](uint64_t value) -> double {
      return metrics_.update_attempts_observed > 0
                 ? static_cast<double>(value) /
                       static_cast<double>(metrics_.update_attempts_observed)
                 : 0.0;
    };
    std::ofstream out(cfg_.metrics_out);
    out << "{\n";
    out << "  \"mode\": \"" << metrics_.mode << "\",\n";
    out << "  \"distance_mode\": \"" << cfg_.distance_mode << "\",\n";
    out << "  \"storage_access_mode\": \""
        << (cfg_.distance_mode == "compute" ? "raw_rados" : "cls") << "\",\n";
    out << "  \"distance_split_probe\": "
        << (cfg_.distance_split_probe ? "true" : "false") << ",\n";
    out << "  \"distance_probe_interval_seconds\": "
        << cfg_.distance_probe_interval_seconds << ",\n";
    out << "  \"vectors_processed\": " << metrics_.vectors_processed << ",\n";
    out << "  \"load_seconds\": " << metrics_.load_seconds << ",\n";
    out << "  \"graph_seconds\": " << metrics_.graph_seconds << ",\n";
    out << "  \"total_seconds\": " << metrics_.total_seconds << ",\n";
    out << "  \"remote_vector_calls\": " << metrics_.remote_vector_calls << ",\n";
    out << "  \"remote_vector_bytes\": " << metrics_.remote_vector_bytes << ",\n";
    out << "  \"remote_adj_calls\": " << metrics_.remote_adj_calls << ",\n";
    out << "  \"remote_distance_calls\": " << metrics_.remote_distance_calls << ",\n";
    out << "  \"remote_noop_calls\": " << metrics_.remote_noop_calls << ",\n";
    out << "  \"remote_patch_calls\": " << metrics_.remote_patch_calls << ",\n";
    out << "  \"remote_candidates_scored\": " << metrics_.remote_candidates_scored << ",\n";
    out << "  \"remote_adj_nodes\": " << metrics_.remote_adj_nodes << ",\n";
    out << "  \"total_patched_nodes\": " << metrics_.total_patched_nodes << ",\n";
    out << "  \"total_neighbor_links\": " << metrics_.total_neighbor_links << ",\n";
    out << "  \"stale_marks\": " << metrics_.stale_marks << ",\n";
    out << "  \"meta_cas_retries\": " << metrics_.meta_cas_retries << ",\n";
    out << "  \"label_cas_conflicts\": " << metrics_.label_cas_conflicts << ",\n";
    out << "  \"failed_updates\": " << metrics_.failed_updates << ",\n";
    out << "  \"rados_exec_retries\": " << metrics_.rados_exec_retries << ",\n";
    out << "  \"osd_op_timeout_seconds\": " << cfg_.osd_op_timeout_seconds << ",\n";
    out << "  \"osd_op_retry_limit\": " << cfg_.osd_op_retry_limit << ",\n";
    out << "  \"failure_breakdown\": {\n";
    out << "    \"timeout\": " << metrics_.failed_update_timeouts << ",\n";
    out << "    \"conflict\": " << metrics_.failed_update_conflicts << ",\n";
    out << "    \"not_found\": " << metrics_.failed_update_not_found << ",\n";
    out << "    \"protocol\": " << metrics_.failed_update_protocol << ",\n";
    out << "    \"other\": " << metrics_.failed_update_other << "\n";
    out << "  },\n";
    out << "  \"failure_operations\": {";
    bool first_failure_operation = true;
    for (const auto& [operation, count] : metrics_.failure_operations) {
      out << (first_failure_operation ? "\n" : ",\n");
      out << "    \"" << operation << "\": " << count;
      first_failure_operation = false;
    }
    if (!first_failure_operation) {
      out << "\n  ";
    }
    out << "},\n";
    out << "  \"time_limit_seconds\": " << metrics_.time_limit_seconds << ",\n";
    out << "  \"update_parallelism\": " << metrics_.update_parallelism << ",\n";
    out << "  \"stopped_by_time_limit\": "
        << (metrics_.stopped_by_time_limit ? "true" : "false") << ",\n";
    out << "  \"throughput_updates_per_sec\": " << metrics_.throughput_updates_per_sec << ",\n";
    out << "  \"avg_update_latency_ms\": " << metrics_.avg_update_latency_ms << ",\n";
    out << "  \"p50_update_latency_ms\": " << metrics_.p50_update_latency_ms << ",\n";
    out << "  \"p95_update_latency_ms\": " << metrics_.p95_update_latency_ms << ",\n";
    out << "  \"p99_update_latency_ms\": " << metrics_.p99_update_latency_ms << ",\n";
    out << "  \"quality\": {\n";
    out << "    \"ground_truth_enabled\": "
        << (cfg_.ground_truth.empty() ? "false" : "true") << ",\n";
    out << "    \"recall_k\": " << cfg_.recall_k << ",\n";
    out << "    \"evaluated_updates\": "
        << metrics_.recall_evaluated_updates << ",\n";
    out << "    \"hits\": " << metrics_.recall_hits << ",\n";
    out << "    \"denominator\": " << metrics_.recall_denominator << ",\n";
    out << "    \"precommit_static_recall_at_k\": "
        << (metrics_.recall_denominator > 0
                ? static_cast<double>(metrics_.recall_hits) /
                      static_cast<double>(metrics_.recall_denominator)
                : 0.0)
        << ",\n";
    out << "    \"paired_query_samples\": [";
    bool first_recall_sample = true;
    for (const auto& [query_index, sample] : metrics_.recall_samples) {
      out << (first_recall_sample ? "\n" : ",\n");
      first_recall_sample = false;
      out << "      {\"query_index\": " << query_index
          << ", \"update_sequence\": " << sample.update_sequence
          << ", \"hits\": " << sample.hits
          << ", \"denominator\": " << sample.denominator << "}";
    }
    if (!first_recall_sample) {
      out << "\n    ";
    }
    out << "],\n";
    out << "    \"semantics\": "
        << "\"level-0 precommit search against original-base ground truth\"\n";
    out << "  },\n";
    out << "  \"operation_profile\": {";
    bool first_operation = true;
    for (const auto& [operation, values] : metrics_.operation_metrics) {
      out << (first_operation ? "\n" : ",\n");
      out << "    \"" << operation << "\": {";
      out << "\"calls\": " << values.calls << ", ";
      out << "\"request_bytes\": " << values.request_bytes << ", ";
      out << "\"reply_bytes\": " << values.reply_bytes << ", ";
      out << "\"roundtrip_seconds\": " << values.roundtrip_seconds << ", ";
      out << "\"avg_roundtrip_ms\": "
          << (values.calls > 0
                  ? values.roundtrip_seconds * 1000.0 /
                        static_cast<double>(values.calls)
                  : 0.0)
          << "}";
      first_operation = false;
    }
    if (!first_operation) {
      out << "\n  ";
    }
    out << "},\n";
    out << "  \"raw_operation_profile\": {";
    bool first_raw_operation = true;
    for (const auto& [operation, values] : metrics_.raw_operation_metrics) {
      out << (first_raw_operation ? "\n" : ",\n");
      out << "    \"" << operation << "\": {";
      out << "\"calls\": " << values.calls << ", ";
      out << "\"request_bytes\": " << values.request_bytes << ", ";
      out << "\"reply_bytes\": " << values.reply_bytes << ", ";
      out << "\"roundtrip_seconds\": " << values.roundtrip_seconds << ", ";
      out << "\"avg_roundtrip_ms\": "
          << (values.calls > 0
                  ? values.roundtrip_seconds * 1000.0 /
                        static_cast<double>(values.calls)
                  : 0.0)
          << "}";
      first_raw_operation = false;
    }
    if (!first_raw_operation) {
      out << "\n  ";
    }
    out << "},\n";
    out << "  \"observability\": {\n";
    out << "    \"protocol_schema_version\": "
        << ghnsw::kProtocolSchemaVersion << ",\n";
    out << "    \"update_attempts\": " << metrics_.update_attempts_observed << ",\n";
    out << "    \"total_cls_exec_calls\": " << metrics_.total_cls_exec_calls << ",\n";
    out << "    \"total_raw_rados_calls\": "
        << metrics_.total_raw_rados_calls << ",\n";
    out << "    \"global_meta_cas_calls\": "
        << metrics_.global_meta_cas_calls << ",\n";
    out << "    \"label_cas_calls\": " << metrics_.label_cas_calls << ",\n";
    out << "    \"cls_calls_per_update_attempt\": "
        << per_attempt(metrics_.total_cls_exec_calls) << ",\n";
    out << "    \"cls_request_bytes\": " << metrics_.cls_request_bytes << ",\n";
    out << "    \"cls_reply_bytes\": " << metrics_.cls_reply_bytes << ",\n";
    out << "    \"raw_rados_calls_per_update_attempt\": "
        << per_attempt(metrics_.total_raw_rados_calls) << ",\n";
    out << "    \"raw_rados_request_bytes\": "
        << metrics_.raw_rados_request_bytes << ",\n";
    out << "    \"raw_rados_reply_bytes\": "
        << metrics_.raw_rados_reply_bytes << ",\n";
    out << "    \"logical_distance_query_bytes\": "
        << metrics_.logical_distance_query_bytes << ",\n";
    out << "    \"distance_batches\": " << metrics_.distance_batches << ",\n";
    out << "    \"candidates_per_distance_batch\": "
        << (metrics_.distance_batches > 0
                ? static_cast<double>(metrics_.remote_candidates_scored) /
                      static_cast<double>(metrics_.distance_batches)
                : 0.0)
        << ",\n";
    out << "    \"max_candidates_per_distance_batch\": "
        << metrics_.max_candidates_per_distance_batch << ",\n";
    out << "    \"distance_batch_size_histogram\": {";
    bool first_distance_batch_size = true;
    for (const auto& [batch_size, count] : metrics_.distance_batch_size_histogram) {
      if (!first_distance_batch_size) {
        out << ", ";
      }
      first_distance_batch_size = false;
      out << "\"" << batch_size << "\": " << count;
    }
    out << "},\n";
    out << "    \"avg_unique_distance_objects_per_update_attempt\": "
        << per_attempt(metrics_.unique_distance_objects_sum) << ",\n";
    out << "    \"max_unique_distance_objects_per_update_attempt\": "
        << metrics_.max_unique_distance_objects << ",\n";
    out << "    \"candidates_per_unique_distance_object\": "
        << (metrics_.unique_distance_objects_sum > 0
                ? static_cast<double>(metrics_.remote_candidates_scored) /
                      static_cast<double>(metrics_.unique_distance_objects_sum)
                : 0.0)
        << ",\n";
    out << "    \"frontier_windows\": " << metrics_.frontier_windows << ",\n";
    out << "    \"frontier_window_fill\": "
        << (metrics_.frontier_windows > 0
                ? static_cast<double>(metrics_.frontier_nodes_selected) /
                      static_cast<double>(metrics_.frontier_windows)
                : 0.0)
        << ",\n";
    out << "    \"distance_candidates_before_dedupe\": "
        << metrics_.distance_candidates_before_dedupe << ",\n";
    out << "    \"distance_candidates_after_dedupe\": "
        << metrics_.distance_candidates_after_dedupe << ",\n";
    out << "    \"distance_candidate_dedupe_retention_ratio\": "
        << (metrics_.distance_candidates_before_dedupe > 0
                ? static_cast<double>(metrics_.distance_candidates_after_dedupe) /
                      static_cast<double>(metrics_.distance_candidates_before_dedupe)
                : 0.0)
        << ",\n";
    out << "    \"avg_unique_data_objects_per_update_attempt\": "
        << per_attempt(metrics_.unique_data_objects_sum) << ",\n";
    out << "    \"max_unique_data_objects_per_update_attempt\": "
        << metrics_.max_unique_data_objects << ",\n";
    out << "    \"avg_unique_data_pgs_per_update_attempt\": "
        << per_attempt(metrics_.unique_data_pgs_sum) << ",\n";
    out << "    \"max_unique_data_pgs_per_update_attempt\": "
        << metrics_.max_unique_data_pgs << ",\n";
    out << "    \"avg_unique_owner_shards_per_update_attempt\": "
        << per_attempt(metrics_.unique_owner_shards_sum) << ",\n";
    out << "    \"max_unique_owner_shards_per_update_attempt\": "
        << metrics_.max_unique_owner_shards << ",\n";
    out << "    \"cross_owner_neighbor_links\": "
        << metrics_.cross_owner_neighbor_links << ",\n";
    out << "    \"cross_owner_neighbor_link_ratio\": "
        << (metrics_.total_neighbor_links > 0
                ? static_cast<double>(metrics_.cross_owner_neighbor_links) /
                      static_cast<double>(metrics_.total_neighbor_links)
                : 0.0)
        << "\n";
    out << "  },\n";
    out << "  \"stage_profile\": {\n";
    out << "    \"lookup_old_seconds\": " << metrics_.lookup_old_seconds << ",\n";
    out << "    \"mark_stale_seconds\": " << metrics_.mark_stale_seconds << ",\n";
    out << "    \"full_graph_search_exclusive_seconds\": "
        << metrics_.full_graph_search_exclusive_seconds << ",\n";
    out << "    \"remote_distance_seconds\": " << metrics_.remote_distance_seconds << ",\n";
    out << "    \"distance_fetch_rpc_seconds\": " << metrics_.distance_fetch_rpc_seconds << ",\n";
    out << "    \"distance_local_compute_seconds\": " << metrics_.distance_local_compute_seconds << ",\n";
    out << "    \"distance_compute_node_unaccounted_seconds\": "
        << metrics_.distance_compute_node_unaccounted_seconds << ",\n";
    out << "    \"remote_vector_seconds\": " << metrics_.remote_vector_seconds << ",\n";
    out << "    \"remote_vector_bytes\": " << metrics_.remote_vector_bytes << ",\n";
    out << "    \"distance_cls_total_seconds\": "
        << metrics_.distance_cls_total_seconds << ",\n";
    out << "    \"distance_vector_ref_seconds\": "
        << metrics_.distance_vector_ref_seconds << ",\n";
    out << "    \"distance_payload_read_seconds\": "
        << metrics_.distance_payload_read_seconds << ",\n";
    out << "    \"distance_compute_seconds\": "
        << metrics_.distance_compute_seconds << ",\n";
    out << "    \"distance_cls_accounted_seconds\": "
        << metrics_.distance_cls_accounted_seconds << ",\n";
    out << "    \"distance_cls_unaccounted_seconds\": "
        << metrics_.distance_cls_unaccounted_seconds << ",\n";
    out << "    \"distance_roundtrip_queue_seconds\": "
        << metrics_.distance_roundtrip_queue_seconds << ",\n";
    out << "    \"distance_noop_roundtrip_seconds\": "
        << metrics_.distance_noop_roundtrip_seconds << ",\n";
    out << "    \"distance_noop_cls_total_seconds\": "
        << metrics_.distance_noop_cls_total_seconds << ",\n";
    out << "    \"distance_network_roundtrip_est_seconds\": "
        << metrics_.distance_network_roundtrip_est_seconds << ",\n";
    out << "    \"distance_osd_queue_est_seconds\": "
        << metrics_.distance_osd_queue_est_seconds << ",\n";
    out << "    \"distance_unaccounted_seconds\": "
        << metrics_.distance_unaccounted_seconds << ",\n";
    out << "    \"adjacency_patch_exclusive_seconds\": "
        << metrics_.adjacency_patch_exclusive_seconds << ",\n";
    out << "    \"global_meta_update_seconds\": "
        << metrics_.global_meta_update_seconds << ",\n";
    out << "    \"other_update_seconds\": " << metrics_.other_update_seconds << ",\n";
    out << "    \"store_vector_seconds\": " << metrics_.store_vector_seconds << ",\n";
    out << "    \"set_new_adjacency_seconds\": " << metrics_.set_new_adjacency_seconds << ",\n";
    out << "    \"meta_read_seconds\": " << metrics_.meta_read_seconds << ",\n";
    out << "    \"patch_prepare_seconds_raw\": " << metrics_.patch_prepare_seconds << ",\n";
    out << "    \"patch_apply_seconds_raw\": " << metrics_.adjacency_patch_seconds << ",\n";
    out << "    \"graph_search_seconds_raw\": " << metrics_.graph_search_seconds << ",\n";
    out << "    \"graph_search_distance_seconds\": " << metrics_.graph_search_distance_seconds << ",\n";
    out << "    \"patch_prepare_distance_seconds\": " << metrics_.patch_prepare_distance_seconds << ",\n";
    out << "    \"accounted_update_seconds\": " << metrics_.accounted_update_seconds << ",\n";
    out << "    \"lookup_old_pct\": " << pct(metrics_.lookup_old_seconds) << ",\n";
    out << "    \"mark_stale_pct\": " << pct(metrics_.mark_stale_seconds) << ",\n";
    out << "    \"full_graph_search_pct\": "
        << pct(metrics_.full_graph_search_exclusive_seconds) << ",\n";
    out << "    \"remote_distance_pct\": " << pct(metrics_.remote_distance_seconds) << ",\n";
    out << "    \"distance_cls_total_pct\": "
        << pct(metrics_.distance_cls_total_seconds) << ",\n";
    out << "    \"distance_fetch_rpc_pct\": "
        << pct(metrics_.distance_fetch_rpc_seconds) << ",\n";
    out << "    \"distance_local_compute_pct\": "
        << pct(metrics_.distance_local_compute_seconds) << ",\n";
    out << "    \"distance_compute_node_unaccounted_pct\": "
        << pct(metrics_.distance_compute_node_unaccounted_seconds) << ",\n";
    out << "    \"distance_vector_ref_pct\": "
        << pct(metrics_.distance_vector_ref_seconds) << ",\n";
    out << "    \"distance_payload_read_pct\": "
        << pct(metrics_.distance_payload_read_seconds) << ",\n";
    out << "    \"distance_compute_pct\": "
        << pct(metrics_.distance_compute_seconds) << ",\n";
    out << "    \"distance_unaccounted_pct\": "
        << pct(metrics_.distance_unaccounted_seconds) << ",\n";
    out << "    \"distance_cls_unaccounted_pct\": "
        << pct(metrics_.distance_cls_unaccounted_seconds) << ",\n";
    out << "    \"distance_roundtrip_queue_pct\": "
        << pct(metrics_.distance_roundtrip_queue_seconds) << ",\n";
    out << "    \"distance_noop_roundtrip_pct\": "
        << pct(metrics_.distance_noop_roundtrip_seconds) << ",\n";
    out << "    \"distance_noop_cls_total_pct\": "
        << pct(metrics_.distance_noop_cls_total_seconds) << ",\n";
    out << "    \"distance_network_roundtrip_est_pct\": "
        << pct(metrics_.distance_network_roundtrip_est_seconds) << ",\n";
    out << "    \"distance_osd_queue_est_pct\": "
        << pct(metrics_.distance_osd_queue_est_seconds) << ",\n";
    out << "    \"adjacency_patch_pct\": "
        << pct(metrics_.adjacency_patch_exclusive_seconds) << ",\n";
    out << "    \"global_meta_update_pct\": "
        << pct(metrics_.global_meta_update_seconds) << ",\n";
    out << "    \"other_update_pct\": " << pct(metrics_.other_update_seconds) << ",\n";
    out << "    \"lookup_old_ms_per_update\": "
        << per_update_ms(metrics_.lookup_old_seconds) << ",\n";
    out << "    \"mark_stale_ms_per_update\": "
        << per_update_ms(metrics_.mark_stale_seconds) << ",\n";
    out << "    \"full_graph_search_ms_per_update\": "
        << per_update_ms(metrics_.full_graph_search_exclusive_seconds) << ",\n";
    out << "    \"remote_distance_ms_per_update\": "
        << per_update_ms(metrics_.remote_distance_seconds) << ",\n";
    out << "    \"distance_cls_total_ms_per_update\": "
        << per_update_ms(metrics_.distance_cls_total_seconds) << ",\n";
    out << "    \"distance_fetch_rpc_ms_per_update\": "
        << per_update_ms(metrics_.distance_fetch_rpc_seconds) << ",\n";
    out << "    \"distance_local_compute_ms_per_update\": "
        << per_update_ms(metrics_.distance_local_compute_seconds) << ",\n";
    out << "    \"distance_compute_node_unaccounted_ms_per_update\": "
        << per_update_ms(metrics_.distance_compute_node_unaccounted_seconds) << ",\n";
    out << "    \"distance_vector_ref_ms_per_update\": "
        << per_update_ms(metrics_.distance_vector_ref_seconds) << ",\n";
    out << "    \"distance_payload_read_ms_per_update\": "
        << per_update_ms(metrics_.distance_payload_read_seconds) << ",\n";
    out << "    \"distance_compute_ms_per_update\": "
        << per_update_ms(metrics_.distance_compute_seconds) << ",\n";
    out << "    \"distance_unaccounted_ms_per_update\": "
        << per_update_ms(metrics_.distance_unaccounted_seconds) << ",\n";
    out << "    \"distance_cls_unaccounted_ms_per_update\": "
        << per_update_ms(metrics_.distance_cls_unaccounted_seconds) << ",\n";
    out << "    \"distance_roundtrip_queue_ms_per_update\": "
        << per_update_ms(metrics_.distance_roundtrip_queue_seconds) << ",\n";
    out << "    \"distance_noop_roundtrip_ms_per_update\": "
        << per_update_ms(metrics_.distance_noop_roundtrip_seconds) << ",\n";
    out << "    \"distance_noop_cls_total_ms_per_update\": "
        << per_update_ms(metrics_.distance_noop_cls_total_seconds) << ",\n";
    out << "    \"distance_network_roundtrip_est_ms_per_update\": "
        << per_update_ms(metrics_.distance_network_roundtrip_est_seconds) << ",\n";
    out << "    \"distance_osd_queue_est_ms_per_update\": "
        << per_update_ms(metrics_.distance_osd_queue_est_seconds) << ",\n";
    out << "    \"adjacency_patch_ms_per_update\": "
        << per_update_ms(metrics_.adjacency_patch_exclusive_seconds) << ",\n";
    out << "    \"global_meta_update_ms_per_update\": "
        << per_update_ms(metrics_.global_meta_update_seconds) << ",\n";
    out << "    \"other_update_ms_per_update\": "
        << per_update_ms(metrics_.other_update_seconds) << "\n";
    out << "  }\n";
    out << "}\n";
  }

  void WriteProgress(uint64_t processed) {
    if (cfg_.progress_out.empty()) {
      return;
    }
    std::ofstream out(cfg_.progress_out);
    out << "{\n";
    out << "  \"mode\": \"" << metrics_.mode << "\",\n";
    out << "  \"processed\": " << processed << ",\n";
    out << "  \"target\": " << (cfg_.mode == "build" ? cfg_.num_vectors : cfg_.num_updates) << ",\n";
    out << "  \"time_limit_seconds\": " << cfg_.time_limit_seconds << ",\n";
    out << "  \"update_parallelism\": " << cfg_.update_parallelism << ",\n";
    out << "  \"distance_mode\": \"" << cfg_.distance_mode << "\",\n";
    out << "  \"distance_split_probe\": "
        << (cfg_.distance_split_probe ? "true" : "false") << ",\n";
    out << "  \"distance_probe_interval_seconds\": "
        << cfg_.distance_probe_interval_seconds << ",\n";
    out << "  \"remote_adj_calls\": " << metrics_.remote_adj_calls << ",\n";
    out << "  \"remote_distance_calls\": " << metrics_.remote_distance_calls << ",\n";
    out << "  \"remote_noop_calls\": " << metrics_.remote_noop_calls << ",\n";
    out << "  \"remote_patch_calls\": " << metrics_.remote_patch_calls << ",\n";
    out << "  \"remote_candidates_scored\": " << metrics_.remote_candidates_scored << ",\n";
    out << "  \"remote_adj_nodes\": " << metrics_.remote_adj_nodes << ",\n";
    out << "  \"total_patched_nodes\": " << metrics_.total_patched_nodes << ",\n";
    out << "  \"total_neighbor_links\": " << metrics_.total_neighbor_links << ",\n";
    out << "  \"recall_evaluated_updates\": "
        << metrics_.recall_evaluated_updates << ",\n";
    out << "  \"recall_hits\": " << metrics_.recall_hits << ",\n";
    out << "  \"recall_denominator\": " << metrics_.recall_denominator << ",\n";
    out << "  \"elapsed_seconds\": " << now_sec() << "\n";
    out << "}\n";
  }

  Config cfg_;
  CephFacade ceph_;
	  Metrics metrics_;
	  std::mt19937_64 rng_;
	  std::atomic<uint64_t> next_update_id_;
	  StripedLocks node_locks_;
	  StripedLocks label_locks_;
	  std::vector<AdjacencyBlob> pending_patches_;
	  std::vector<double> update_latencies_ms_;
	  std::vector<std::vector<uint32_t>> ground_truth_;
	};

Config ParseArgs(int argc, const char** argv) {
  Config cfg;
  auto parse_vector_kind = [](const std::string& value) -> uint32_t {
    if (value == "u8") return ghnsw::kVectorKindU8;
    if (value == "f32") return ghnsw::kVectorKindF32;
    throw std::runtime_error("unsupported vector kind: " + value);
  };
  auto parse_metric = [](const std::string& value) -> uint32_t {
    if (value == "l2") return ghnsw::kMetricL2;
    if (value == "ip") return ghnsw::kMetricIP;
    throw std::runtime_error("unsupported metric: " + value);
  };
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    auto next = [&](const char* name) -> std::string {
      if (i + 1 >= argc) {
        throw std::runtime_error(std::string("missing value for ") + name);
      }
      return argv[++i];
    };
    if (arg == "--mode") {
      cfg.mode = next("--mode");
    } else if (arg == "--input") {
      cfg.input = next("--input");
    } else if (arg == "--input-format") {
      cfg.input_format = next("--input-format");
    } else if (arg == "--update-input") {
      cfg.update_input = next("--update-input");
    } else if (arg == "--update-input-format") {
      cfg.update_input_format = next("--update-input-format");
    } else if (arg == "--keyring") {
      cfg.keyring = next("--keyring");
    } else if (arg == "--meta-pool") {
      cfg.meta_pool = next("--meta-pool");
    } else if (arg == "--owner-pool-prefix") {
      cfg.owner_pool_prefix = next("--owner-pool-prefix");
    } else if (arg == "--data-oid") {
      cfg.data_oid = next("--data-oid");
    } else if (arg == "--meta-oid") {
      cfg.meta_oid = next("--meta-oid");
    } else if (arg == "--num-vectors") {
      cfg.num_vectors = std::stoull(next("--num-vectors"));
    } else if (arg == "--num-updates") {
      cfg.num_updates = std::stoull(next("--num-updates"));
    } else if (arg == "--update-offset") {
      cfg.update_offset = std::stoull(next("--update-offset"));
    } else if (arg == "--target-start") {
      cfg.target_start = std::stoull(next("--target-start"));
    } else if (arg == "--owners") {
      cfg.owners = static_cast<uint32_t>(std::stoul(next("--owners")));
    } else if (arg == "--points-per-object") {
      cfg.points_per_object = std::stoull(next("--points-per-object"));
    } else if (arg == "--dim") {
      cfg.dim = static_cast<uint32_t>(std::stoul(next("--dim")));
    } else if (arg == "--M") {
      cfg.M = static_cast<uint32_t>(std::stoul(next("--M")));
    } else if (arg == "--ef") {
      cfg.ef = static_cast<uint32_t>(std::stoul(next("--ef")));
    } else if (arg == "--metrics-out") {
      cfg.metrics_out = next("--metrics-out");
    } else if (arg == "--progress-out") {
      cfg.progress_out = next("--progress-out");
    } else if (arg == "--ground-truth") {
      cfg.ground_truth = next("--ground-truth");
    } else if (arg == "--recall-k") {
      cfg.recall_k = static_cast<uint32_t>(std::stoul(next("--recall-k")));
      if (cfg.recall_k == 0) {
        throw std::runtime_error("--recall-k must be >= 1");
      }
    } else if (arg == "--vector-kind") {
      cfg.vector_kind = parse_vector_kind(next("--vector-kind"));
    } else if (arg == "--metric") {
      cfg.metric = parse_metric(next("--metric"));
    } else if (arg == "--distance-mode") {
      cfg.distance_mode = next("--distance-mode");
      if (cfg.distance_mode != "osd" && cfg.distance_mode != "compute") {
        throw std::runtime_error("--distance-mode must be osd or compute");
      }
    } else if (arg == "--distance-split-probe") {
      cfg.distance_split_probe = true;
    } else if (arg == "--distance-probe-interval-ms") {
      const double interval_ms = std::stod(next("--distance-probe-interval-ms"));
      if (interval_ms <= 0.0) {
        throw std::runtime_error("--distance-probe-interval-ms must be > 0");
      }
      cfg.distance_probe_interval_seconds = interval_ms / 1000.0;
    } else if (arg == "--time-limit-seconds") {
      cfg.time_limit_seconds = std::stoull(next("--time-limit-seconds"));
    } else if (arg == "--update-parallelism") {
      cfg.update_parallelism = static_cast<uint32_t>(std::stoul(next("--update-parallelism")));
      if (cfg.update_parallelism == 0) {
        throw std::runtime_error("--update-parallelism must be >= 1");
      }
    } else if (arg == "--osd-op-timeout-seconds") {
      cfg.osd_op_timeout_seconds =
          static_cast<uint32_t>(std::stoul(next("--osd-op-timeout-seconds")));
      if (cfg.osd_op_timeout_seconds == 0) {
        throw std::runtime_error("--osd-op-timeout-seconds must be >= 1");
      }
    } else if (arg == "--osd-op-retry-limit") {
      cfg.osd_op_retry_limit =
          static_cast<uint32_t>(std::stoul(next("--osd-op-retry-limit")));
    }
  }
  return cfg;
}

}  // namespace

int main(int argc, const char** argv) {
  try {
    Config cfg = ParseArgs(argc, argv);
    Coordinator coordinator(cfg);
    coordinator.Run();
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << std::endl;
    return 1;
  }
}
