#include "include/ceph_assert.h"
#include "objclass/objclass.h"

#include <algorithm>
#include <cmath>
#include <chrono>
#include <map>
#include <limits>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "nsvu/protocol.hpp"

namespace {

using ghnsw::AdjacencyBlob;
using ghnsw::CasGlobalMetaRequest;
using ghnsw::CasLabelRequest;
using ghnsw::DistanceBatchReply;
using ghnsw::DistanceBatchRequest;
using ghnsw::EdgePatchBatchRequest;
using ghnsw::GetAdjBatchReply;
using ghnsw::GetGlobalMetaReply;
using ghnsw::GetVectorBatchReply;
using ghnsw::GlobalMeta;
using ghnsw::IdBatchRequest;
using ghnsw::LabelBatchRequest;
using ghnsw::LabelUpdateBatchRequest;
using ghnsw::LookupLabelBatchReply;
using ghnsw::MarkNodeStaleRequest;
using ghnsw::SetAdjacencyBatchRequest;
using ghnsw::StatusReply;
using ghnsw::StoreVectorReply;
using ghnsw::StoreVectorRequest;
using ghnsw::TimedNoopReply;
using ghnsw::VectorRef;

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
int decode_msg(ceph::bufferlist* in, T* out) {
  auto it = in->cbegin();
  try {
    out->decode(it);
  } catch (ceph::buffer::error&) {
    return -EINVAL;
  }
  return 0;
}

template <typename T>
void encode_msg(const T& in, ceph::bufferlist* out) {
  in.encode(*out);
}

double cls_now_sec() {
  using Clock = std::chrono::steady_clock;
  static const auto start = Clock::now();
  const auto delta = Clock::now() - start;
  return std::chrono::duration_cast<std::chrono::duration<double>>(delta).count();
}

ceph::bufferlist encode_u64(uint64_t value) {
  ceph::bufferlist bl;
  ceph::encode(value, bl);
  return bl;
}

ceph::bufferlist encode_u32(uint32_t value) {
  ceph::bufferlist bl;
  ceph::encode(value, bl);
  return bl;
}

int decode_u64(const ceph::bufferlist& bl, uint64_t* value) {
  auto it = bl.cbegin();
  try {
    ceph::decode(*value, it);
  } catch (ceph::buffer::error&) {
    return -EINVAL;
  }
  return 0;
}

int decode_u32(const ceph::bufferlist& bl, uint32_t* value) {
  auto it = bl.cbegin();
  try {
    ceph::decode(*value, it);
  } catch (ceph::buffer::error&) {
    return -EINVAL;
  }
  return 0;
}

int read_vector_ref(cls_method_context_t hctx, uint64_t global_id, VectorRef* ref) {
  ceph::bufferlist bl;
  int r = cls_cxx_map_get_val(hctx, ghnsw::VecKey(global_id), &bl);
  if (r < 0) {
    return r;
  }
  auto it = bl.cbegin();
  try {
    ref->decode(it);
  } catch (ceph::buffer::error&) {
    return -EINVAL;
  }
  return 0;
}

int read_adjacency(cls_method_context_t hctx, uint64_t global_id, AdjacencyBlob* adj) {
  ceph::bufferlist bl;
  int r = cls_cxx_map_get_val(hctx, ghnsw::NodeKey(global_id), &bl);
  if (r < 0) {
    return r;
  }
  auto it = bl.cbegin();
  try {
    adj->decode(it);
  } catch (ceph::buffer::error&) {
    return -EINVAL;
  }
  return 0;
}

int write_global_meta(cls_method_context_t hctx, const GlobalMeta& meta) {
  std::map<std::string, ceph::bufferlist> kv;
  kv.emplace(kMetaEnterPoint, encode_u64(meta.enterpoint));
  kv.emplace(kMetaMaxLevel, encode_u32(meta.max_level));
  kv.emplace(kMetaCurCount, encode_u64(meta.cur_element_count));
  kv.emplace(kMetaNextGlobal, encode_u64(meta.next_global_id));
  kv.emplace(kMetaVersion, encode_u64(meta.version));
  kv.emplace(kMetaM, encode_u32(meta.M));
  kv.emplace(kMetaEf, encode_u32(meta.ef));
  kv.emplace(kMetaDim, encode_u32(meta.dim));
  kv.emplace(kMetaVectorKind, encode_u32(meta.vector_kind));
  kv.emplace(kMetaMetric, encode_u32(meta.metric));
  return cls_cxx_map_set_vals(hctx, &kv);
}

int read_global_meta(cls_method_context_t hctx, GlobalMeta* meta) {
  ceph::bufferlist bl;
  int r = cls_cxx_map_get_val(hctx, kMetaVersion, &bl);
  if (r == -ENOENT) {
    *meta = GlobalMeta{};
    return 0;
  }
  if (r < 0) {
    return r;
  }
  if ((r = decode_u64(bl, &meta->version)) < 0) {
    return r;
  }

  if ((r = cls_cxx_map_get_val(hctx, kMetaEnterPoint, &bl)) < 0) {
    return r;
  }
  if ((r = decode_u64(bl, &meta->enterpoint)) < 0) {
    return r;
  }
  if ((r = cls_cxx_map_get_val(hctx, kMetaMaxLevel, &bl)) < 0) {
    return r;
  }
  if ((r = decode_u32(bl, &meta->max_level)) < 0) {
    return r;
  }
  if ((r = cls_cxx_map_get_val(hctx, kMetaCurCount, &bl)) < 0) {
    return r;
  }
  if ((r = decode_u64(bl, &meta->cur_element_count)) < 0) {
    return r;
  }
  if ((r = cls_cxx_map_get_val(hctx, kMetaNextGlobal, &bl)) < 0) {
    return r;
  }
  if ((r = decode_u64(bl, &meta->next_global_id)) < 0) {
    return r;
  }
  if ((r = cls_cxx_map_get_val(hctx, kMetaM, &bl)) < 0) {
    return r;
  }
  if ((r = decode_u32(bl, &meta->M)) < 0) {
    return r;
  }
  if ((r = cls_cxx_map_get_val(hctx, kMetaEf, &bl)) < 0) {
    return r;
  }
  if ((r = decode_u32(bl, &meta->ef)) < 0) {
    return r;
  }
  if ((r = cls_cxx_map_get_val(hctx, kMetaDim, &bl)) < 0) {
    return r;
  }
  if ((r = decode_u32(bl, &meta->dim)) < 0) {
    return r;
  }
  r = cls_cxx_map_get_val(hctx, kMetaVectorKind, &bl);
  if (r == -ENOENT) {
    meta->vector_kind = ghnsw::kVectorKindU8;
  } else if (r < 0) {
    return r;
  } else if ((r = decode_u32(bl, &meta->vector_kind)) < 0) {
    return r;
  }
  r = cls_cxx_map_get_val(hctx, kMetaMetric, &bl);
  if (r == -ENOENT) {
    meta->metric = ghnsw::kMetricL2;
    return 0;
  }
  if (r < 0) {
    return r;
  }
  return decode_u32(bl, &meta->metric);
}

float l2_u8(const std::string& a, const std::string& b, uint32_t dim) {
  const uint8_t* pa = reinterpret_cast<const uint8_t*>(a.data());
  const uint8_t* pb = reinterpret_cast<const uint8_t*>(b.data());
  float sum = 0.0f;
  for (uint32_t i = 0; i < dim; ++i) {
    float d = static_cast<float>(pa[i]) - static_cast<float>(pb[i]);
    sum += d * d;
  }
  return sum;
}

float l2_f32(const std::string& a, const std::string& b, uint32_t dim) {
  const float* pa = reinterpret_cast<const float*>(a.data());
  const float* pb = reinterpret_cast<const float*>(b.data());
  float sum = 0.0f;
  for (uint32_t i = 0; i < dim; ++i) {
    const float d = pa[i] - pb[i];
    sum += d * d;
  }
  return sum;
}

float ip_distance_f32(const std::string& a, const std::string& b, uint32_t dim) {
  const float* pa = reinterpret_cast<const float*>(a.data());
  const float* pb = reinterpret_cast<const float*>(b.data());
  float dot = 0.0f;
  for (uint32_t i = 0; i < dim; ++i) {
    dot += pa[i] * pb[i];
  }
  return 1.0f - dot;
}

float compute_distance(
    const std::string& query,
    const std::string& vec,
    uint32_t dim,
    uint32_t vector_kind,
    uint32_t metric) {
  if (vector_kind == ghnsw::kVectorKindU8) {
    return l2_u8(query, vec, dim);
  }
  if (vector_kind == ghnsw::kVectorKindF32 && metric == ghnsw::kMetricL2) {
    return l2_f32(query, vec, dim);
  }
  if (vector_kind == ghnsw::kVectorKindF32 && metric == ghnsw::kMetricIP) {
    return ip_distance_f32(query, vec, dim);
  }
  return std::numeric_limits<float>::max();
}

int cls_store_vector(cls_method_context_t hctx, ceph::bufferlist* in, ceph::bufferlist* out) {
  StoreVectorRequest req;
  int r = decode_msg(in, &req);
  if (r < 0) {
    return r;
  }
  uint64_t size = 0;
  r = cls_cxx_stat2(hctx, &size, nullptr);
  if (r == -ENOENT) {
    size = 0;
    r = 0;
  }
  if (r < 0) {
    return r;
  }

  ceph::bufferlist payload;
  payload.append(req.vector_bytes);
  r = cls_cxx_write2(
      hctx, static_cast<int>(size), static_cast<int>(payload.length()), &payload, 0);
  if (r < 0) {
    return r;
  }

  VectorRef ref;
  ref.global_id = req.global_id;
  ref.external_label = req.external_label;
  ref.offset = size;
  ref.bytes = static_cast<uint32_t>(req.vector_bytes.size());
  ref.dim = req.dim;
  ref.vector_kind = req.vector_kind;
  ref.flags = req.flags;
  ref.level = req.level;

  std::map<std::string, ceph::bufferlist> kv;
  ceph::bufferlist ref_bl;
  ref.encode(ref_bl);
  kv.emplace(ghnsw::VecKey(req.global_id), std::move(ref_bl));
  r = cls_cxx_map_set_vals(hctx, &kv);

  StatusReply reply;
  reply.status = r;
  reply.count = r < 0 ? 0 : 1;
  encode_msg(reply, out);
  return r;
}

int cls_update_label_batch(cls_method_context_t hctx, ceph::bufferlist* in, ceph::bufferlist* out) {
  LabelUpdateBatchRequest req;
  int r = decode_msg(in, &req);
  if (r < 0) {
    return r;
  }
  std::map<std::string, ceph::bufferlist> kv;
  for (const auto& entry : req.entries) {
    kv.emplace(ghnsw::LabelKey(entry.first), encode_u64(entry.second));
  }
  r = cls_cxx_map_set_vals(hctx, &kv);
  StatusReply reply;
  reply.status = r;
  reply.count = static_cast<uint32_t>(req.entries.size());
  encode_msg(reply, out);
  return r;
}

int cls_cas_label(cls_method_context_t hctx, ceph::bufferlist* in, ceph::bufferlist* out) {
  CasLabelRequest req;
  int r = decode_msg(in, &req);
  if (r < 0) {
    return r;
  }

  ceph::bufferlist current_bl;
  r = cls_cxx_map_get_val(hctx, ghnsw::LabelKey(req.external_label), &current_bl);
  bool matches = false;
  if (req.expect_missing) {
    matches = r == -ENOENT;
  } else if (r == 0) {
    uint64_t current = 0;
    r = decode_u64(current_bl, &current);
    if (r < 0) {
      return r;
    }
    matches = current == req.expected_global_id;
  } else if (r != -ENOENT) {
    return r;
  }

  StatusReply reply;
  if (!matches) {
    reply.status = -EAGAIN;
    encode_msg(reply, out);
    return -EAGAIN;
  }
  std::map<std::string, ceph::bufferlist> values;
  values.emplace(
      ghnsw::LabelKey(req.external_label), encode_u64(req.replacement_global_id));
  r = cls_cxx_map_set_vals(hctx, &values);
  reply.status = r;
  reply.count = r < 0 ? 0 : 1;
  encode_msg(reply, out);
  return r;
}

int cls_get_node_vector_batch(cls_method_context_t hctx, ceph::bufferlist* in, ceph::bufferlist* out) {
  IdBatchRequest req;
  int r = decode_msg(in, &req);
  if (r < 0) {
    return r;
  }
  GetVectorBatchReply reply;
  for (uint64_t id : req.global_ids) {
    VectorRef ref;
    r = read_vector_ref(hctx, id, &ref);
    if (r == -ENOENT) {
      continue;
    }
    if (r < 0) {
      reply.status = r;
      encode_msg(reply, out);
      return r;
    }
    ceph::bufferlist bl;
    r = cls_cxx_read2(hctx, static_cast<int>(ref.offset), static_cast<int>(ref.bytes), &bl, 0);
    if (r < 0) {
      reply.status = r;
      encode_msg(reply, out);
      return r;
    }
    reply.values.emplace_back(id, std::string(bl.c_str(), bl.length()));
  }
  encode_msg(reply, out);
  return 0;
}

int cls_lookup_label_batch(cls_method_context_t hctx, ceph::bufferlist* in, ceph::bufferlist* out) {
  LabelBatchRequest req;
  int r = decode_msg(in, &req);
  if (r < 0) {
    return r;
  }
  LookupLabelBatchReply reply;
  for (uint64_t label : req.external_labels) {
    ceph::bufferlist bl;
    r = cls_cxx_map_get_val(hctx, ghnsw::LabelKey(label), &bl);
    if (r == -ENOENT) {
      continue;
    }
    if (r < 0) {
      reply.status = r;
      encode_msg(reply, out);
      return r;
    }
    uint64_t global_id = 0;
    r = decode_u64(bl, &global_id);
    if (r < 0) {
      reply.status = r;
      encode_msg(reply, out);
      return r;
    }
    reply.values.emplace_back(label, global_id);
  }
  encode_msg(reply, out);
  return 0;
}

int cls_get_node_adjacency_batch(
    cls_method_context_t hctx, ceph::bufferlist* in, ceph::bufferlist* out) {
  IdBatchRequest req;
  int r = decode_msg(in, &req);
  if (r < 0) {
    return r;
  }
  GetAdjBatchReply reply;
  for (uint64_t id : req.global_ids) {
    ceph::bufferlist bl;
    r = cls_cxx_map_get_val(hctx, ghnsw::NodeKey(id), &bl);
    if (r == -ENOENT) {
      continue;
    }
    if (r < 0) {
      reply.status = r;
      encode_msg(reply, out);
      return r;
    }
    AdjacencyBlob adj;
    auto it = bl.cbegin();
    try {
      adj.decode(it);
    } catch (ceph::buffer::error&) {
      reply.status = -EINVAL;
      encode_msg(reply, out);
      return -EINVAL;
    }
    reply.values.emplace_back(id, std::move(adj));
  }
  encode_msg(reply, out);
  return 0;
}

int cls_distance_to_local_batch(
    cls_method_context_t hctx, ceph::bufferlist* in, ceph::bufferlist* out) {
  const double cls_total_t0 = cls_now_sec();
  DistanceBatchRequest req;
  int r = decode_msg(in, &req);
  if (r < 0) {
    return r;
  }
  DistanceBatchReply reply;
  for (uint64_t id : req.global_ids) {
    VectorRef ref;
    double t0 = cls_now_sec();
    r = read_vector_ref(hctx, id, &ref);
    reply.vector_ref_seconds += cls_now_sec() - t0;
    if (r == -ENOENT) {
      continue;
    }
    if (r < 0) {
      reply.status = r;
      reply.cls_total_seconds = cls_now_sec() - cls_total_t0;
      encode_msg(reply, out);
      return r;
    }
    ceph::bufferlist bl;
    t0 = cls_now_sec();
    r = cls_cxx_read2(hctx, static_cast<int>(ref.offset), static_cast<int>(ref.bytes), &bl, 0);
    reply.payload_read_seconds += cls_now_sec() - t0;
    if (r < 0) {
      reply.status = r;
      reply.cls_total_seconds = cls_now_sec() - cls_total_t0;
      encode_msg(reply, out);
      return r;
    }
    std::string vec(bl.c_str(), bl.length());
    t0 = cls_now_sec();
    const float dist = compute_distance(req.query_vector, vec, req.dim, req.vector_kind, req.metric);
    reply.compute_seconds += cls_now_sec() - t0;
    reply.distances.emplace_back(
        id, dist);
  }
  reply.cls_total_seconds = cls_now_sec() - cls_total_t0;
  encode_msg(reply, out);
  return 0;
}

int cls_timed_noop(cls_method_context_t, ceph::bufferlist*, ceph::bufferlist* out) {
  const double t0 = cls_now_sec();
  TimedNoopReply reply;
  reply.status = 0;
  reply.cls_total_seconds = cls_now_sec() - t0;
  encode_msg(reply, out);
  return 0;
}

int set_adjacency_common(
    cls_method_context_t hctx, ceph::bufferlist* in, ceph::bufferlist* out) {
  SetAdjacencyBatchRequest req;
  int r = decode_msg(in, &req);
  if (r < 0) {
    return r;
  }
  std::map<std::string, ceph::bufferlist> kv;
  for (const auto& entry : req.entries) {
    ceph::bufferlist bl;
    entry.encode(bl);
    kv.emplace(ghnsw::NodeKey(entry.global_id), std::move(bl));
  }
  r = cls_cxx_map_set_vals(hctx, &kv);
  StatusReply reply;
  reply.status = r;
  reply.count = static_cast<uint32_t>(req.entries.size());
  encode_msg(reply, out);
  return r;
}

int cls_apply_edge_patch_batch(
    cls_method_context_t hctx, ceph::bufferlist* in, ceph::bufferlist* out) {
  EdgePatchBatchRequest req;
  int r = decode_msg(in, &req);
  if (r < 0) {
    return r;
  }
  if (req.max_neighbors == 0) {
    return -EINVAL;
  }
  std::map<std::string, ceph::bufferlist> kv;
  std::map<uint64_t, AdjacencyBlob> merged_patches;
  for (const auto& patch : req.entries) {
    auto& dst_patch = merged_patches[patch.global_id];
    dst_patch.global_id = patch.global_id;
    if (dst_patch.neighbors.size() < patch.neighbors.size()) {
      dst_patch.neighbors.resize(patch.neighbors.size());
    }
    for (size_t level = 0; level < patch.neighbors.size(); ++level) {
      auto& dst = dst_patch.neighbors[level];
      for (uint64_t id : patch.neighbors[level]) {
        if (std::find(dst.begin(), dst.end(), id) == dst.end()) {
          dst.push_back(id);
        }
      }
    }
    dst_patch.level_count = static_cast<uint32_t>(dst_patch.neighbors.size());
  }
  for (const auto& [_, patch] : merged_patches) {
    AdjacencyBlob merged;
    r = read_adjacency(hctx, patch.global_id, &merged);
    if (r == -ENOENT) {
      merged = patch;
    } else if (r < 0) {
      StatusReply reply;
      reply.status = r;
      reply.count = 0;
      encode_msg(reply, out);
      return r;
    } else {
      if (merged.neighbors.size() < patch.neighbors.size()) {
        merged.neighbors.resize(patch.neighbors.size());
      }
      for (size_t level = 0; level < patch.neighbors.size(); ++level) {
        auto& dst = merged.neighbors[level];
        for (uint64_t id : patch.neighbors[level]) {
          if (std::find(dst.begin(), dst.end(), id) == dst.end()) {
            dst.push_back(id);
          }
        }
        const size_t max_m =
            level == 0 ? static_cast<size_t>(req.max_neighbors) * 2 : req.max_neighbors;
        if (dst.size() > max_m) {
          dst.erase(dst.begin(), dst.end() - static_cast<std::ptrdiff_t>(max_m));
        }
      }
      merged.level_count = static_cast<uint32_t>(merged.neighbors.size());
    }
    ceph::bufferlist bl;
    merged.encode(bl);
    kv.emplace(ghnsw::NodeKey(merged.global_id), std::move(bl));
  }
  r = cls_cxx_map_set_vals(hctx, &kv);
  StatusReply reply;
  reply.status = r;
  reply.count = static_cast<uint32_t>(req.entries.size());
  encode_msg(reply, out);
  return r;
}

int cls_mark_node_stale(cls_method_context_t hctx, ceph::bufferlist* in, ceph::bufferlist* out) {
  MarkNodeStaleRequest req;
  int r = decode_msg(in, &req);
  if (r < 0) {
    return r;
  }
  VectorRef ref;
  r = read_vector_ref(hctx, req.global_id, &ref);
  if (r < 0) {
    return r;
  }
  ref.flags = ghnsw::kVectorFlagStale;
  ceph::bufferlist bl;
  ref.encode(bl);
  std::map<std::string, ceph::bufferlist> kv;
  kv.emplace(ghnsw::VecKey(req.global_id), std::move(bl));
  r = cls_cxx_map_set_vals(hctx, &kv);
  StatusReply reply;
  reply.status = r;
  reply.count = r < 0 ? 0 : 1;
  encode_msg(reply, out);
  return r;
}

int cls_get_global_meta(cls_method_context_t hctx, ceph::bufferlist*, ceph::bufferlist* out) {
  GlobalMeta meta;
  int r = read_global_meta(hctx, &meta);
  GetGlobalMetaReply reply;
  reply.status = r;
  reply.meta = meta;
  encode_msg(reply, out);
  return r;
}

int cls_cas_global_meta(cls_method_context_t hctx, ceph::bufferlist* in, ceph::bufferlist* out) {
  CasGlobalMetaRequest req;
  int r = decode_msg(in, &req);
  if (r < 0) {
    return r;
  }
  GlobalMeta current;
  r = read_global_meta(hctx, &current);
  if (r < 0) {
    return r;
  }
  StatusReply reply;
  if (current.version != req.expected_version) {
    reply.status = -EAGAIN;
    reply.count = 0;
    encode_msg(reply, out);
    return -EAGAIN;
  }
  r = write_global_meta(hctx, req.meta);
  reply.status = r;
  reply.count = r < 0 ? 0 : 1;
  encode_msg(reply, out);
  return r;
}

}  // namespace

CLS_INIT(hnsw_global) {
  cls_handle_t h_class;
  cls_method_handle_t h_store_vector;
  cls_method_handle_t h_update_label_batch;
  cls_method_handle_t h_cas_label;
  cls_method_handle_t h_get_node_vector_batch;
  cls_method_handle_t h_lookup_label_batch;
  cls_method_handle_t h_get_node_adjacency_batch;
  cls_method_handle_t h_distance_to_local_batch;
  cls_method_handle_t h_timed_noop;
  cls_method_handle_t h_set_adjacency_batch;
  cls_method_handle_t h_apply_edge_patch_batch;
  cls_method_handle_t h_mark_node_stale;
  cls_method_handle_t h_get_global_meta;
  cls_method_handle_t h_cas_global_meta;

  cls_register("hnsw_global", &h_class);
  cls_register_cxx_method(
      h_class, "store_vector", CLS_METHOD_RD | CLS_METHOD_WR, cls_store_vector, &h_store_vector);
  cls_register_cxx_method(
      h_class,
      "update_label_batch",
      CLS_METHOD_RD | CLS_METHOD_WR,
      cls_update_label_batch,
      &h_update_label_batch);
  cls_register_cxx_method(
      h_class,
      "cas_label",
      CLS_METHOD_RD | CLS_METHOD_WR,
      cls_cas_label,
      &h_cas_label);
  cls_register_cxx_method(
      h_class,
      "get_node_vector_batch",
      CLS_METHOD_RD,
      cls_get_node_vector_batch,
      &h_get_node_vector_batch);
  cls_register_cxx_method(
      h_class,
      "lookup_label_batch",
      CLS_METHOD_RD,
      cls_lookup_label_batch,
      &h_lookup_label_batch);
  cls_register_cxx_method(
      h_class,
      "get_node_adjacency_batch",
      CLS_METHOD_RD,
      cls_get_node_adjacency_batch,
      &h_get_node_adjacency_batch);
  cls_register_cxx_method(
      h_class,
      "distance_to_local_batch",
      CLS_METHOD_RD,
      cls_distance_to_local_batch,
      &h_distance_to_local_batch);
  cls_register_cxx_method(
      h_class,
      "timed_noop",
      CLS_METHOD_RD,
      cls_timed_noop,
      &h_timed_noop);
  cls_register_cxx_method(
      h_class,
      "set_adjacency_batch",
      CLS_METHOD_RD | CLS_METHOD_WR,
      set_adjacency_common,
      &h_set_adjacency_batch);
  cls_register_cxx_method(
      h_class,
      "apply_edge_patch_batch",
      CLS_METHOD_RD | CLS_METHOD_WR,
      cls_apply_edge_patch_batch,
      &h_apply_edge_patch_batch);
  cls_register_cxx_method(
      h_class,
      "mark_node_stale",
      CLS_METHOD_RD | CLS_METHOD_WR,
      cls_mark_node_stale,
      &h_mark_node_stale);
  cls_register_cxx_method(
      h_class, "get_global_meta", CLS_METHOD_RD, cls_get_global_meta, &h_get_global_meta);
  cls_register_cxx_method(
      h_class,
      "cas_global_meta",
      CLS_METHOD_RD | CLS_METHOD_WR,
      cls_cas_global_meta,
      &h_cas_global_meta);
}
