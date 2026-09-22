#pragma once

#include <cstdio>
#include <stdint.h>

#include <string>
#include <utility>
#include <vector>

#include "include/encoding.h"

namespace ghnsw {

static constexpr uint32_t kVectorFlagActive = 1u;
static constexpr uint32_t kVectorFlagStale = 2u;
static constexpr uint32_t kVectorKindU8 = 1u;
static constexpr uint32_t kVectorKindF32 = 2u;
static constexpr uint32_t kMetricL2 = 1u;
static constexpr uint32_t kMetricIP = 2u;

struct VectorRef {
  uint64_t global_id = 0;
  uint64_t external_label = 0;
  uint64_t offset = 0;
  uint32_t bytes = 0;
  uint32_t dim = 0;
  uint32_t vector_kind = kVectorKindU8;
  uint32_t flags = 0;
  uint32_t level = 0;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(global_id, bl);
    ceph::encode(external_label, bl);
    ceph::encode(offset, bl);
    ceph::encode(bytes, bl);
    ceph::encode(dim, bl);
    ceph::encode(vector_kind, bl);
    ceph::encode(flags, bl);
    ceph::encode(level, bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(global_id, it);
    ceph::decode(external_label, it);
    ceph::decode(offset, it);
    ceph::decode(bytes, it);
    ceph::decode(dim, it);
    ceph::decode(vector_kind, it);
    ceph::decode(flags, it);
    ceph::decode(level, it);
  }
};

struct AdjacencyBlob {
  uint64_t global_id = 0;
  uint32_t level_count = 0;
  std::vector<std::vector<uint64_t>> neighbors;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(global_id, bl);
    ceph::encode(level_count, bl);
    ceph::encode(neighbors, bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(global_id, it);
    ceph::decode(level_count, it);
    ceph::decode(neighbors, it);
  }
};

struct GlobalMeta {
  uint64_t enterpoint = UINT64_MAX;
  uint32_t max_level = 0;
  uint64_t cur_element_count = 0;
  uint64_t next_global_id = 0;
  uint64_t version = 0;
  uint32_t M = 8;
  uint32_t ef = 32;
  uint32_t dim = 128;
  uint32_t vector_kind = kVectorKindU8;
  uint32_t metric = kMetricL2;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(enterpoint, bl);
    ceph::encode(max_level, bl);
    ceph::encode(cur_element_count, bl);
    ceph::encode(next_global_id, bl);
    ceph::encode(version, bl);
    ceph::encode(M, bl);
    ceph::encode(ef, bl);
    ceph::encode(dim, bl);
    ceph::encode(vector_kind, bl);
    ceph::encode(metric, bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(enterpoint, it);
    ceph::decode(max_level, it);
    ceph::decode(cur_element_count, it);
    ceph::decode(next_global_id, it);
    ceph::decode(version, it);
    ceph::decode(M, it);
    ceph::decode(ef, it);
    ceph::decode(dim, it);
    ceph::decode(vector_kind, it);
    ceph::decode(metric, it);
  }
};

struct StoreVectorRequest {
  uint64_t global_id = 0;
  uint64_t external_label = 0;
  uint32_t dim = 0;
  uint32_t vector_kind = kVectorKindU8;
  uint32_t flags = 0;
  uint32_t level = 0;
  std::string vector_bytes;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(global_id, bl);
    ceph::encode(external_label, bl);
    ceph::encode(dim, bl);
    ceph::encode(vector_kind, bl);
    ceph::encode(flags, bl);
    ceph::encode(level, bl);
    ceph::encode(vector_bytes, bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(global_id, it);
    ceph::decode(external_label, it);
    ceph::decode(dim, it);
    ceph::decode(vector_kind, it);
    ceph::decode(flags, it);
    ceph::decode(level, it);
    ceph::decode(vector_bytes, it);
  }
};

struct StoreVectorReply {
  int32_t status = 0;
  VectorRef ref;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(status, bl);
    ref.encode(bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(status, it);
    ref.decode(it);
  }
};

struct IdBatchRequest {
  std::vector<uint64_t> global_ids;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(global_ids, bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(global_ids, it);
  }
};

struct LabelBatchRequest {
  std::vector<uint64_t> external_labels;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(external_labels, bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(external_labels, it);
  }
};

struct GetVectorBatchReply {
  int32_t status = 0;
  std::vector<std::pair<uint64_t, std::string>> values;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(status, bl);
    ceph::encode(values, bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(status, it);
    ceph::decode(values, it);
  }
};

struct GetAdjBatchReply {
  int32_t status = 0;
  std::vector<std::pair<uint64_t, AdjacencyBlob>> values;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(status, bl);
    ceph::encode(static_cast<uint32_t>(values.size()), bl);
    for (const auto& kv : values) {
      ceph::encode(kv.first, bl);
      kv.second.encode(bl);
    }
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(status, it);
    uint32_t count = 0;
    ceph::decode(count, it);
    values.clear();
    values.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
      uint64_t key = 0;
      AdjacencyBlob value;
      ceph::decode(key, it);
      value.decode(it);
      values.emplace_back(key, std::move(value));
    }
  }
};

struct LookupLabelBatchReply {
  int32_t status = 0;
  std::vector<std::pair<uint64_t, uint64_t>> values;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(status, bl);
    ceph::encode(values, bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(status, it);
    ceph::decode(values, it);
  }
};

struct LabelUpdateBatchRequest {
  std::vector<std::pair<uint64_t, uint64_t>> entries;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(entries, bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(entries, it);
  }
};

struct CasLabelRequest {
  uint64_t external_label = 0;
  uint64_t expected_global_id = 0;
  uint64_t replacement_global_id = 0;
  bool expect_missing = false;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(external_label, bl);
    ceph::encode(expected_global_id, bl);
    ceph::encode(replacement_global_id, bl);
    ceph::encode(expect_missing, bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(external_label, it);
    ceph::decode(expected_global_id, it);
    ceph::decode(replacement_global_id, it);
    ceph::decode(expect_missing, it);
  }
};

struct ReserveInsertRequest {
  uint64_t update_id = 0;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(update_id, bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(update_id, it);
  }
};

struct ReserveInsertReply {
  int32_t status = 0;
  uint64_t global_id = 0;
  GlobalMeta search_meta;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(status, bl);
    ceph::encode(global_id, bl);
    search_meta.encode(bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(status, it);
    ceph::decode(global_id, it);
    search_meta.decode(it);
  }
};

struct FinalizeInsertRequest {
  uint64_t update_id = 0;
  uint64_t global_id = 0;
  uint32_t level = 0;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(update_id, bl);
    ceph::encode(global_id, bl);
    ceph::encode(level, bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(update_id, it);
    ceph::decode(global_id, it);
    ceph::decode(level, it);
  }
};

struct DistanceBatchRequest {
  std::string query_vector;
  uint32_t dim = 0;
  uint32_t vector_kind = kVectorKindU8;
  uint32_t metric = kMetricL2;
  std::vector<uint64_t> global_ids;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(query_vector, bl);
    ceph::encode(dim, bl);
    ceph::encode(vector_kind, bl);
    ceph::encode(metric, bl);
    ceph::encode(global_ids, bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(query_vector, it);
    ceph::decode(dim, it);
    ceph::decode(vector_kind, it);
    ceph::decode(metric, it);
    ceph::decode(global_ids, it);
  }
};

struct DistanceBatchReply {
  int32_t status = 0;
  std::vector<std::pair<uint64_t, float>> distances;
  double cls_total_seconds = 0.0;
  double vector_ref_seconds = 0.0;
  double payload_read_seconds = 0.0;
  double compute_seconds = 0.0;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(status, bl);
    ceph::encode(distances, bl);
    ceph::encode(cls_total_seconds, bl);
    ceph::encode(vector_ref_seconds, bl);
    ceph::encode(payload_read_seconds, bl);
    ceph::encode(compute_seconds, bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(status, it);
    ceph::decode(distances, it);
    try {
      ceph::decode(cls_total_seconds, it);
      ceph::decode(vector_ref_seconds, it);
      ceph::decode(payload_read_seconds, it);
      ceph::decode(compute_seconds, it);
    } catch (ceph::buffer::error&) {
      cls_total_seconds = 0.0;
      vector_ref_seconds = 0.0;
      payload_read_seconds = 0.0;
      compute_seconds = 0.0;
    }
  }
};

struct TimedNoopReply {
  int32_t status = 0;
  double cls_total_seconds = 0.0;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(status, bl);
    ceph::encode(cls_total_seconds, bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(status, it);
    try {
      ceph::decode(cls_total_seconds, it);
    } catch (ceph::buffer::error&) {
      cls_total_seconds = 0.0;
    }
  }
};

struct SetAdjacencyBatchRequest {
  std::vector<AdjacencyBlob> entries;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(static_cast<uint32_t>(entries.size()), bl);
    for (const auto& entry : entries) {
      entry.encode(bl);
    }
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    uint32_t count = 0;
    ceph::decode(count, it);
    entries.clear();
    entries.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
      AdjacencyBlob entry;
      entry.decode(it);
      entries.push_back(std::move(entry));
    }
  }
};

struct EdgePatchBatchRequest {
  uint64_t update_id = 0;
  uint32_t max_neighbors = 8;
  std::vector<AdjacencyBlob> entries;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(update_id, bl);
    ceph::encode(max_neighbors, bl);
    ceph::encode(static_cast<uint32_t>(entries.size()), bl);
    for (const auto& entry : entries) {
      entry.encode(bl);
    }
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(update_id, it);
    ceph::decode(max_neighbors, it);
    uint32_t count = 0;
    ceph::decode(count, it);
    entries.clear();
    entries.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
      AdjacencyBlob entry;
      entry.decode(it);
      entries.push_back(std::move(entry));
    }
  }
};

struct StatusReply {
  int32_t status = 0;
  uint32_t count = 0;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(status, bl);
    ceph::encode(count, bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(status, it);
    ceph::decode(count, it);
  }
};

struct MarkNodeStaleRequest {
  uint64_t global_id = 0;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(global_id, bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(global_id, it);
  }
};

struct GetGlobalMetaReply {
  int32_t status = 0;
  GlobalMeta meta;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(status, bl);
    meta.encode(bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(status, it);
    meta.decode(it);
  }
};

struct CasGlobalMetaRequest {
  uint64_t expected_version = 0;
  GlobalMeta meta;

  void encode(ceph::buffer::list& bl) const {
    ceph::encode(expected_version, bl);
    meta.encode(bl);
  }

  void decode(ceph::buffer::list::const_iterator& it) {
    ceph::decode(expected_version, it);
    meta.decode(it);
  }
};

inline std::string VecKey(uint64_t global_id) {
  return "vec/" + std::to_string(global_id);
}

inline std::string NodeKey(uint64_t global_id) {
  return "node/" + std::to_string(global_id);
}

inline std::string LabelKey(uint64_t external_label) {
  return "label/" + std::to_string(external_label);
}

inline std::string ReservationKey(uint64_t update_id) {
  return "reservation/" + std::to_string(update_id);
}

inline std::string FinalizedKey(uint64_t update_id) {
  return "finalized/" + std::to_string(update_id);
}

inline std::string PatchKey(uint64_t update_id) {
  return "patch/" + std::to_string(update_id);
}

inline std::string OwnerMetaOid() {
  return "hnsw.owner.meta";
}

inline std::string OwnerDataOid(uint64_t chunk_id) {
  char buf[64];
  snprintf(buf, sizeof(buf), "hnsw.owner.data.%06llu",
           static_cast<unsigned long long>(chunk_id));
  return std::string(buf);
}

}  // namespace ghnsw
