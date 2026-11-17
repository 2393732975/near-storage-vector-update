#include <rados/librados.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <unordered_set>

#include <omp.h>

#include "include/ceph_assert.h"
#include "nsvu/placement.hpp"
#include "nsvu/import_batches.hpp"
#include "nsvu/protocol.hpp"
#include <hnswlib/hnswlib.h>

namespace {

using ghnsw::AdjacencyBlob;
using ghnsw::GlobalMeta;
using ghnsw::VectorRef;

struct Config {
  std::string input;
  std::string input_format = "u8bin";
  std::string keyring;
  std::string meta_pool = "nsvu_meta";
  std::string owner_pool_prefix = "nsvu_owner_";
  std::string data_oid = "hnsw.owner.data";
  std::string meta_oid = "hnsw.global.meta";
  std::string metrics_out;
  std::string progress_out;
  std::string index_cache;
  std::string index_cache_mode = "disabled";
  std::string dataset_sha256;
  bool build_only = false;
  bool help = false;
  std::string query_input;
  std::string ground_truth;
  std::string local_recall_out;
  uint64_t query_count = 0;
  uint64_t query_offset = 0;
  uint32_t recall_k = 10;
  uint32_t query_ef = 128;
  uint64_t num_vectors = 1000000;
  uint32_t dim = 128;
  uint32_t owners = 5;
  uint64_t points_per_object = 250000;
  uint32_t M = 8;
  uint32_t ef = 32;
  uint32_t threads = 32;
  uint32_t seed = 100;
  uint32_t import_workers = 1;
  uint32_t osd_op_timeout_seconds = 45;
  uint32_t omap_batch = 2048;
  uint64_t omap_batch_bytes = 8ull * 1024ull * 1024ull;
  uint64_t payload_batch_bytes = 64ull * 1024ull * 1024ull;
  uint32_t vector_kind = ghnsw::kVectorKindU8;
  uint32_t metric = ghnsw::kMetricL2;
};

struct Metrics {
  uint64_t vectors_imported = 0;
  double total_seconds = 0.0;
  double load_seconds = 0.0;
  double convert_seconds = 0.0;
  double build_seconds = 0.0;
  double hnsw_build_seconds = 0.0;
  double cache_save_seconds = 0.0;
  double cache_load_seconds = 0.0;
  bool index_cache_loaded = false;
  double id_mapping_seconds = 0.0;
  uint64_t permuted_internal_ids = 0;
  double vector_persist_seconds = 0.0;
  double adjacency_persist_seconds = 0.0;
  double meta_persist_seconds = 0.0;
  uint64_t omap_write_calls = 0;
  uint64_t omap_application_bytes = 0;
  uint64_t payload_write_calls = 0;
  uint64_t payload_write_bytes = 0;
  uint64_t object_reset_calls = 0;
  double omap_rpc_seconds = 0;
  double payload_rpc_seconds = 0;
  std::vector<double> owner_vector_seconds;
  std::vector<double> owner_adjacency_seconds;
};

void WriteProgress(
    const Config& cfg,
    const std::string& stage,
    uint64_t processed,
    uint64_t total,
    double elapsed_seconds) {
  if (cfg.progress_out.empty()) {
    return;
  }
  const std::string temporary = cfg.progress_out + ".tmp";
  std::ofstream out(temporary);
  if (!out) throw std::runtime_error("cannot write importer progress");
  out << "{\n";
  out << "  \"mode\": \"import_base\",\n";
  out << "  \"stage\": \"" << stage << "\",\n";
  out << "  \"processed\": " << processed << ",\n";
  out << "  \"total\": " << total << ",\n";
  out << "  \"elapsed_seconds\": " << elapsed_seconds << "\n";
  out << "}\n";
  out.close();
  if (!out || std::rename(temporary.c_str(), cfg.progress_out.c_str()) != 0) {
    throw std::runtime_error("cannot publish importer progress");
  }
}

template <typename T>
ceph::bufferlist EncodeMsg(const T& msg) {
  ceph::bufferlist bl;
  msg.encode(bl);
  return bl;
}

ceph::bufferlist EncodeU64(uint64_t value) {
  ceph::bufferlist bl;
  ceph::encode(value, bl);
  return bl;
}

ceph::bufferlist EncodeU32(uint32_t value) {
  ceph::bufferlist bl;
  ceph::encode(value, bl);
  return bl;
}

double NowSec() {
  using Clock = std::chrono::steady_clock;
  static const auto start = Clock::now();
  return std::chrono::duration_cast<std::chrono::duration<double>>(Clock::now() - start).count();
}

void Ensure(int ret, const std::string& what) {
  if (ret < 0) {
    throw std::runtime_error(what + " failed: " + std::to_string(ret));
  }
}

std::map<std::string, std::string> CacheIdentity(const Config& cfg) {
  return {{"format", "nsvu-hnsw-cache-v1"},
      {"num_vectors", std::to_string(cfg.num_vectors)},
      {"dim", std::to_string(cfg.dim)}, {"M", std::to_string(cfg.M)},
      {"ef_construction", std::to_string(std::max(cfg.ef, cfg.M))},
      {"vector_kind", std::to_string(cfg.vector_kind)},
      {"metric", std::to_string(cfg.metric)},
      {"dataset_sha256", cfg.dataset_sha256}};
}

void WriteCacheIdentity(const Config& cfg) {
  std::ofstream out(cfg.index_cache + ".meta");
  if (!out) throw std::runtime_error("cannot write cache identity");
  for (const auto& kv : CacheIdentity(cfg)) out << kv.first << '=' << kv.second << '\n';
  out << "seed=" << cfg.seed << "\nthreads=" << cfg.threads << '\n';
  out.close();
  if (!out) throw std::runtime_error("cannot finish cache identity");
}

void ValidateCacheIdentity(const Config& cfg) {
  std::ifstream in(cfg.index_cache + ".meta");
  if (!in) throw std::runtime_error("cache identity sidecar missing; refusing unverified cache");
  std::map<std::string, std::string> values;
  std::string line;
  while (std::getline(in, line)) {
    auto split = line.find('=');
    if (split == std::string::npos || !values.emplace(line.substr(0, split), line.substr(split + 1)).second) {
      throw std::runtime_error("invalid cache identity sidecar");
    }
  }
  for (const auto& kv : CacheIdentity(cfg)) {
    if (values.find(kv.first) == values.end() || values.at(kv.first) != kv.second) {
      throw std::runtime_error("cache identity mismatch: " + kv.first);
    }
  }
}

std::vector<std::string> OwnerPools(const Config& cfg) {
  std::vector<std::string> pools;
  pools.reserve(cfg.owners);
  for (uint32_t i = 0; i < cfg.owners; ++i) {
    pools.push_back(cfg.owner_pool_prefix + std::to_string(i));
  }
  return pools;
}

size_t element_size_for_kind(uint32_t vector_kind) {
  return vector_kind == ghnsw::kVectorKindF32 ? sizeof(float) : sizeof(uint8_t);
}

std::vector<std::string> LoadVectors(const Config& cfg, uint64_t offset = 0) {
  std::ifstream in(cfg.input, std::ios::binary);
  if (!in) {
    throw std::runtime_error("failed to open dataset: " + cfg.input);
  }
  const size_t elem_size = element_size_for_kind(cfg.vector_kind);
  const size_t vec_bytes = static_cast<size_t>(cfg.dim) * elem_size;
  if (cfg.input_format == "fbin") {
    uint32_t n = 0;
    uint32_t file_dim = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(uint32_t));
    in.read(reinterpret_cast<char*>(&file_dim), sizeof(uint32_t));
    if (!in) {
      throw std::runtime_error("failed to read fbin header: " + cfg.input);
    }
    if (file_dim != cfg.dim) {
      throw std::runtime_error("fbin dim mismatch");
    }
    if (offset > n || cfg.num_vectors > n - offset) {
      throw std::runtime_error("requested vectors exceed fbin count");
    }
  } else if (cfg.input_format == "u8bin") {
    uint32_t n = 0;
    uint32_t file_dim = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(uint32_t));
    in.read(reinterpret_cast<char*>(&file_dim), sizeof(uint32_t));
    if (!in) {
      throw std::runtime_error("failed to read u8bin header: " + cfg.input);
    }
    if (file_dim != cfg.dim) {
      throw std::runtime_error("u8bin dim mismatch");
    }
    if (offset > n || cfg.num_vectors > n - offset) {
      throw std::runtime_error("requested vectors exceed u8bin count");
    }
  } else {
    throw std::runtime_error("unsupported input format: " + cfg.input_format);
  }
  if (offset > (static_cast<uint64_t>(std::numeric_limits<std::streamoff>::max()) - 8) / vec_bytes) {
    throw std::runtime_error("dataset offset overflow");
  }
  in.seekg(static_cast<std::streamoff>(8 + offset * vec_bytes));
  std::vector<std::string> out;
  out.reserve(cfg.num_vectors);
  std::string buf(vec_bytes, '\0');
  for (uint64_t i = 0; i < cfg.num_vectors; ++i) {
    in.read(buf.data(), static_cast<std::streamsize>(vec_bytes));
    if (!in) {
      throw std::runtime_error("dataset read failed at vector " + std::to_string(i));
    }
    out.push_back(buf);
  }
  return out;
}

void ValidateVectorExtent(const Config& cfg) {
  std::ifstream in(cfg.input, std::ios::binary | std::ios::ate);
  if (!in) throw std::runtime_error("failed to open dataset: " + cfg.input);
  const auto bytes = in.tellg();
  in.seekg(0);
  uint32_t count = 0, dim = 0;
  in.read(reinterpret_cast<char*>(&count), 4);
  in.read(reinterpret_cast<char*>(&dim), 4);
  const uint64_t needed = 8 + cfg.num_vectors * cfg.dim * element_size_for_kind(cfg.vector_kind);
  if (!in || dim != cfg.dim || count < cfg.num_vectors || bytes < 0 ||
      static_cast<uint64_t>(bytes) < needed) {
    throw std::runtime_error("dataset header/extent does not contain requested base N");
  }
}

std::vector<float> MakeFloatVectors(const std::vector<std::string>& vectors, uint32_t dim) {
  std::vector<float> out;
  out.resize(static_cast<size_t>(vectors.size()) * dim);
#pragma omp parallel for schedule(static)
  for (size_t i = 0; i < vectors.size(); ++i) {
    const uint8_t* src = reinterpret_cast<const uint8_t*>(vectors[i].data());
    float* dst = out.data() + i * dim;
    for (uint32_t d = 0; d < dim; ++d) {
      dst[d] = static_cast<float>(src[d]);
    }
  }
  return out;
}

std::vector<float> MakeFloatVectorsFromKind(
    const std::vector<std::string>& vectors, uint32_t dim, uint32_t vector_kind) {
  if (vector_kind == ghnsw::kVectorKindF32) {
    std::vector<float> out;
    out.resize(static_cast<size_t>(vectors.size()) * dim);
#pragma omp parallel for schedule(static)
    for (size_t i = 0; i < vectors.size(); ++i) {
      std::memcpy(
          out.data() + i * dim,
          vectors[i].data(),
          static_cast<size_t>(dim) * sizeof(float));
    }
    return out;
  }
  return MakeFloatVectors(vectors, dim);
}

AdjacencyBlob BuildAdjacencyBlob(
    const hnswlib::HierarchicalNSW<float>& index,
    hnswlib::tableint internal_id,
    uint64_t global_id) {
  AdjacencyBlob blob;
  blob.global_id = global_id;
  blob.level_count = static_cast<uint32_t>(index.element_levels_[internal_id] + 1);
  blob.neighbors.resize(blob.level_count);
  for (uint32_t level = 0; level < blob.level_count; ++level) {
    auto* ll = level == 0 ? index.get_linklist0(internal_id)
                          : index.get_linklist(internal_id, static_cast<int>(level));
    const uint32_t neighbor_count = *ll;
    auto* neighbors = reinterpret_cast<hnswlib::tableint*>(ll + 1);
    auto& out = blob.neighbors[level];
    out.reserve(neighbor_count);
    for (uint32_t i = 0; i < neighbor_count; ++i) {
      out.push_back(static_cast<uint64_t>(index.getExternalLabel(neighbors[i])));
    }
  }
  return blob;
}

void EvaluateLocalRecall(
    const Config& cfg, hnswlib::HierarchicalNSW<float>& index) {
  if (cfg.local_recall_out.empty()) return;
  Config query_cfg = cfg;
  query_cfg.input = cfg.query_input;
  std::ifstream header(cfg.query_input, std::ios::binary);
  uint32_t query_rows = 0, query_dim = 0;
  header.read(reinterpret_cast<char*>(&query_rows), 4);
  header.read(reinterpret_cast<char*>(&query_dim), 4);
  if (!header || query_dim != cfg.dim || cfg.query_offset >= query_rows) {
    throw std::runtime_error("invalid local recall query header/offset");
  }
  query_cfg.num_vectors = cfg.query_count ? cfg.query_count : query_rows - cfg.query_offset;
  auto queries = LoadVectors(query_cfg, cfg.query_offset);
  auto floats = MakeFloatVectorsFromKind(queries, cfg.dim, cfg.vector_kind);
  std::ifstream gt(cfg.ground_truth, std::ios::binary);
  uint32_t gt_rows = 0, gt_width = 0;
  gt.read(reinterpret_cast<char*>(&gt_rows), 4);
  gt.read(reinterpret_cast<char*>(&gt_width), 4);
  if (!gt || cfg.recall_k > gt_width || gt_width > cfg.num_vectors || cfg.query_offset > gt_rows ||
      queries.size() > gt_rows - cfg.query_offset) {
    throw std::runtime_error("ground truth header/count mismatch");
  }
  const uint64_t gt_row_bytes = static_cast<uint64_t>(gt_width) * sizeof(uint32_t);
  if (cfg.query_offset > (static_cast<uint64_t>(std::numeric_limits<std::streamoff>::max()) - 8) / gt_row_bytes) {
    throw std::runtime_error("ground truth offset overflow");
  }
  gt.seekg(static_cast<std::streamoff>(8 + cfg.query_offset * gt_row_bytes));
  std::ofstream out(cfg.local_recall_out);
  if (!out) throw std::runtime_error("cannot open local recall output");
  index.setEf(cfg.query_ef);
  uint64_t hits = 0;
  out << std::setprecision(17);
  out << "{\n  \"evaluation\": \"local_hnsw_screen_only\",\n"
      << "  \"recall_k\": " << cfg.recall_k << ",\n"
      << "  \"query_ef\": " << cfg.query_ef << ",\n  \"queries\": [\n";
  for (size_t q = 0; q < queries.size(); ++q) {
    std::vector<uint32_t> truth(gt_width);
    gt.read(reinterpret_cast<char*>(truth.data()), static_cast<std::streamsize>(gt_width) * 4);
    if (!gt) throw std::runtime_error("truncated local recall ground truth");
    auto found = index.searchKnn(floats.data() + q * cfg.dim, cfg.recall_k);
    std::vector<std::pair<float, hnswlib::labeltype>> neighbors;
    while (!found.empty()) { neighbors.push_back(found.top()); found.pop(); }
    std::reverse(neighbors.begin(), neighbors.end());
    std::unordered_set<uint64_t> labels;
    for (auto neighbor : neighbors) labels.insert(neighbor.second);
    uint64_t query_hits = 0;
    for (uint32_t k = 0; k < cfg.recall_k; ++k) {
      if (truth[k] >= cfg.num_vectors) throw std::runtime_error("ground truth ID outside actual base N");
      query_hits += labels.count(truth[k]);
    }
    hits += query_hits;
    out << (q ? ",\n" : "") << "    {\"query_index\": " << (q + cfg.query_offset)
        << ", \"hits\": " << query_hits << ", \"denominator\": " << cfg.recall_k
        << ", \"result_labels\": [";
    for (size_t j = 0; j < neighbors.size(); ++j) out << (j ? ", " : "") << neighbors[j].second;
    out << "]}";
  }
  const uint64_t denominator = queries.size() * cfg.recall_k;
  out << "\n  ],\n  \"evaluated_queries\": " << queries.size()
      << ",\n  \"hits\": " << hits << ",\n  \"denominator\": " << denominator
      << ",\n  \"recall_at_k\": " << static_cast<double>(hits) / denominator << "\n}\n";
  out.close();
  if (!out) throw std::runtime_error("cannot finish local recall output");
}

struct PersistStats {
  uint64_t omap_calls = 0, omap_bytes = 0, payload_calls = 0, payload_bytes = 0;
  uint64_t reset_calls = 0;
  double omap_seconds = 0, payload_seconds = 0;
};

void FlushOmapBatch(
    librados::IoCtx& ioctx,
    const std::string& oid,
    std::map<std::string, librados::bufferlist>* kv,
    PersistStats* stats) {
  if (kv->empty()) {
    return;
  }
  const double start = NowSec();
  Ensure(ioctx.omap_set(oid, *kv), "omap_set " + oid);
  stats->omap_seconds += NowSec() - start;
  stats->omap_calls++;
  for (const auto& pair : *kv) stats->omap_bytes += pair.first.size() + pair.second.length();
  kv->clear();
}

class OmapBatch {
 public:
  OmapBatch(const Config& cfg, librados::IoCtx& ioctx, PersistStats* stats)
      : cfg_(cfg), ioctx_(ioctx), stats_(stats) {}
  void ResetObject(const std::string& oid) { Flush(); oid_ = oid; }
  void Add(const std::string& key, librados::bufferlist value) {
    const uint64_t next_bytes = nsvu::OmapEntryBytes(key.size(), value.length());
    if (nsvu::OmapMustFlush(values_.size(), bytes_, next_bytes, cfg_.omap_batch, cfg_.omap_batch_bytes)) Flush();
    if (!values_.emplace(key, std::move(value)).second) throw std::runtime_error("duplicate import OMAP key");
    bytes_ += next_bytes;
    if (values_.size() >= cfg_.omap_batch || bytes_ >= cfg_.omap_batch_bytes) Flush();
  }
  void Flush() {
    FlushOmapBatch(ioctx_, oid_, &values_, stats_);
    bytes_ = 0;
  }
 private:
  const Config& cfg_;
  librados::IoCtx& ioctx_;
  PersistStats* stats_;
  std::string oid_;
  std::map<std::string, librados::bufferlist> values_;
  uint64_t bytes_ = 0;
};

void FlushPayloadBatch(
    librados::IoCtx& ioctx,
    const std::string& oid,
    uint64_t offset,
    ceph::bufferlist* bl,
    PersistStats* stats) {
  if (bl->length() == 0) {
    return;
  }
  const double start = NowSec();
  Ensure(ioctx.write(oid, *bl, bl->length(), offset), "write payload chunk " + oid);
  stats->payload_seconds += NowSec() - start;
  stats->payload_calls++;
  stats->payload_bytes += bl->length();
  bl->clear();
}

void WriteMetrics(const Config& cfg, const Metrics& metrics) {
  if (cfg.metrics_out.empty()) {
    return;
  }
  std::ofstream out(cfg.metrics_out);
  if (!out) throw std::runtime_error("cannot write importer metrics");
  out << std::setprecision(17);
  out << "{\n";
  out << "  \"mode\": \"" << (cfg.build_only ? "build_only" : "import_base") << "\",\n";
  out << "  \"M\": " << cfg.M << ",\n";
  out << "  \"ef_construction\": " << std::max(cfg.M, cfg.ef) << ",\n";
  out << "  \"seed\": " << cfg.seed << ",\n";
  out << "  \"build_threads\": " << cfg.threads << ",\n";
  out << "  \"import_workers\": " << cfg.import_workers << ",\n";
  out << "  \"osd_op_timeout_seconds\": " << cfg.osd_op_timeout_seconds << ",\n";
  out << "  \"omap_batch\": " << cfg.omap_batch << ",\n";
  out << "  \"omap_batch_bytes\": " << cfg.omap_batch_bytes << ",\n";
  out << "  \"num_vectors\": " << cfg.num_vectors << ",\n";
  out << "  \"dim\": " << cfg.dim << ",\n";
  out << "  \"vector_kind\": " << cfg.vector_kind << ",\n";
  out << "  \"metric\": " << cfg.metric << ",\n";
  out << "  \"owners\": " << cfg.owners << ",\n";
  out << "  \"points_per_object\": " << cfg.points_per_object << ",\n";
  out << "  \"payload_batch_bytes\": " << cfg.payload_batch_bytes << ",\n";
  out << "  \"vectors_imported\": " << metrics.vectors_imported << ",\n";
  out << "  \"load_seconds\": " << metrics.load_seconds << ",\n";
  out << "  \"convert_seconds\": " << metrics.convert_seconds << ",\n";
  out << "  \"build_seconds\": " << metrics.build_seconds << ",\n";
  out << "  \"hnsw_build_seconds\": " << metrics.hnsw_build_seconds << ",\n";
  out << "  \"cache_save_seconds\": " << metrics.cache_save_seconds << ",\n";
  out << "  \"cache_load_seconds\": " << metrics.cache_load_seconds << ",\n";
  out << "  \"index_cache_mode\": \"" << cfg.index_cache_mode << "\",\n";
  out << "  \"index_cache_loaded\": "
      << (metrics.index_cache_loaded ? "true" : "false") << ",\n";
  out << "  \"id_mapping_seconds\": " << metrics.id_mapping_seconds << ",\n";
  out << "  \"permuted_internal_ids\": " << metrics.permuted_internal_ids << ",\n";
  out << "  \"vector_persist_seconds\": " << metrics.vector_persist_seconds << ",\n";
  out << "  \"adjacency_persist_seconds\": " << metrics.adjacency_persist_seconds << ",\n";
  out << "  \"meta_persist_seconds\": " << metrics.meta_persist_seconds << ",\n";
  out << "  \"omap_write_calls\": " << metrics.omap_write_calls << ",\n";
  out << "  \"omap_application_bytes\": " << metrics.omap_application_bytes << ",\n";
  out << "  \"payload_write_calls\": " << metrics.payload_write_calls << ",\n";
  out << "  \"payload_write_bytes\": " << metrics.payload_write_bytes << ",\n";
  out << "  \"object_reset_calls\": " << metrics.object_reset_calls << ",\n";
  out << "  \"omap_rpc_worker_seconds\": " << metrics.omap_rpc_seconds << ",\n";
  out << "  \"payload_rpc_worker_seconds\": " << metrics.payload_rpc_seconds << ",\n";
  auto write_owner_times = [&](const char* key, const std::vector<double>& values) {
    out << "  \"" << key << "\": [";
    for (size_t i = 0; i < values.size(); ++i) out << (i ? ", " : "") << values[i];
    out << "],\n";
  };
  write_owner_times("owner_vector_seconds", metrics.owner_vector_seconds);
  write_owner_times("owner_adjacency_seconds", metrics.owner_adjacency_seconds);
  out << "  \"total_seconds\": " << metrics.total_seconds << "\n";
  out << "}\n";
  out.close();
  if (!out) throw std::runtime_error("cannot finish importer metrics");
}

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
    auto number = [&](const char* name) -> uint64_t {
      const std::string raw = next(name);
      if (raw.empty() || raw.find_first_not_of("0123456789") != std::string::npos) {
        throw std::runtime_error(std::string(name) + " requires an unsigned integer");
      }
      return std::stoull(raw);
    };
    auto number32 = [&](const char* name) -> uint32_t {
      const auto value = number(name);
      if (value > UINT32_MAX) throw std::runtime_error(std::string(name) + " overflows uint32");
      return static_cast<uint32_t>(value);
    };
    if (arg == "--help") {
      cfg.help = true;
    } else if (arg == "--build-only") {
      cfg.build_only = true;
    } else if (arg == "--dataset-sha256") {
      cfg.dataset_sha256 = next("--dataset-sha256");
    } else if (arg == "--query-input") {
      cfg.query_input = next("--query-input");
    } else if (arg == "--ground-truth") {
      cfg.ground_truth = next("--ground-truth");
    } else if (arg == "--local-recall-out") {
      cfg.local_recall_out = next("--local-recall-out");
    } else if (arg == "--query-count") {
      cfg.query_count = number("--query-count");
    } else if (arg == "--query-offset") {
      cfg.query_offset = number("--query-offset");
    } else if (arg == "--query-ef") {
      cfg.query_ef = number32("--query-ef");
    } else if (arg == "--recall-k") {
      cfg.recall_k = number32("--recall-k");
    } else if (arg == "--input") {
      cfg.input = next("--input");
    } else if (arg == "--input-format") {
      cfg.input_format = next("--input-format");
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
    } else if (arg == "--metrics-out") {
      cfg.metrics_out = next("--metrics-out");
    } else if (arg == "--progress-out") {
      cfg.progress_out = next("--progress-out");
    } else if (arg == "--index-cache") {
      cfg.index_cache = next("--index-cache");
    } else if (arg == "--index-cache-mode") {
      cfg.index_cache_mode = next("--index-cache-mode");
    } else if (arg == "--num-vectors") {
      cfg.num_vectors = number("--num-vectors");
    } else if (arg == "--dim") {
      cfg.dim = number32("--dim");
    } else if (arg == "--owners") {
      cfg.owners = number32("--owners");
    } else if (arg == "--points-per-object") {
      cfg.points_per_object = number("--points-per-object");
    } else if (arg == "--M") {
      cfg.M = number32("--M");
    } else if (arg == "--ef") {
      cfg.ef = number32("--ef");
    } else if (arg == "--threads") {
      cfg.threads = number32("--threads");
    } else if (arg == "--seed") {
      cfg.seed = number32("--seed");
    } else if (arg == "--import-workers") {
      cfg.import_workers = number32("--import-workers");
    } else if (arg == "--osd-op-timeout-seconds") {
      cfg.osd_op_timeout_seconds = number32("--osd-op-timeout-seconds");
    } else if (arg == "--omap-batch") {
      cfg.omap_batch = number32("--omap-batch");
    } else if (arg == "--omap-batch-bytes") {
      cfg.omap_batch_bytes = number("--omap-batch-bytes");
    } else if (arg == "--payload-batch-bytes") {
      cfg.payload_batch_bytes = number("--payload-batch-bytes");
    } else if (arg == "--vector-kind") {
      cfg.vector_kind = parse_vector_kind(next("--vector-kind"));
    } else if (arg == "--metric") {
      cfg.metric = parse_metric(next("--metric"));
    } else {
      throw std::runtime_error("unknown argument: " + arg);
    }
  }
  if (cfg.help) return cfg;
  if (cfg.input.empty() || cfg.num_vectors == 0 || cfg.num_vectors > UINT32_MAX ||
      cfg.dim == 0 || static_cast<uint64_t>(cfg.dim) * element_size_for_kind(cfg.vector_kind) > UINT32_MAX ||
      cfg.owners == 0 || cfg.points_per_object == 0 || cfg.M < 2 || cfg.M > 10000 || cfg.ef == 0 ||
      cfg.threads == 0 || cfg.threads > 1024 || cfg.import_workers == 0 || cfg.import_workers > cfg.owners ||
      cfg.osd_op_timeout_seconds == 0 || cfg.osd_op_timeout_seconds > 3600 ||
      cfg.omap_batch == 0 || cfg.omap_batch_bytes == 0 || cfg.omap_batch_bytes > 64ull * 1024 * 1024 ||
      cfg.payload_batch_bytes == 0 || cfg.payload_batch_bytes > 64ull * 1024 * 1024 ||
      cfg.recall_k == 0 || (!cfg.local_recall_out.empty() &&
          (cfg.recall_k > cfg.num_vectors || cfg.query_ef < cfg.recall_k))) {
    throw std::runtime_error("invalid importer dimensions, graph or batching parameters");
  }
  if ((cfg.input_format == "fbin") != (cfg.vector_kind == ghnsw::kVectorKindF32) ||
      (cfg.input_format != "fbin" && cfg.input_format != "u8bin")) {
    throw std::runtime_error("input format and vector kind mismatch");
  }
  if (!cfg.dataset_sha256.empty() && (cfg.dataset_sha256.size() != 64 ||
      cfg.dataset_sha256.find_first_not_of("0123456789abcdefABCDEF") != std::string::npos)) {
    throw std::runtime_error("invalid dataset SHA-256");
  }
  if ((!cfg.local_recall_out.empty() && (cfg.query_input.empty() || cfg.ground_truth.empty())) ||
      (cfg.local_recall_out.empty() && (!cfg.query_input.empty() || !cfg.ground_truth.empty()))) {
    throw std::runtime_error("local recall needs query-input, ground-truth and local-recall-out together");
  }
  if (cfg.index_cache.empty() != (cfg.index_cache_mode == "disabled")) {
    throw std::runtime_error(
        "--index-cache and --index-cache-mode=build|load must be provided together");
  }
  if (cfg.index_cache_mode != "disabled" &&
      cfg.index_cache_mode != "build" && cfg.index_cache_mode != "load") {
    throw std::runtime_error("--index-cache-mode must be disabled, build, or load");
  }
  return cfg;
}

}  // namespace

int main(int argc, const char** argv) {
  try {
    Config cfg = ParseArgs(argc, argv);
    if (cfg.help) {
      std::cout << "nsvu-base-importer --input FILE --input-format fbin|u8bin --vector-kind f32|u8\n"
          "  --num-vectors N --dim D --metric l2|ip --M M --ef EF_CONSTRUCTION\n"
          "  [--build-only] [--index-cache FILE --index-cache-mode build|load]\n"
          "  [--threads N --seed N --import-workers N --omap-batch N --omap-batch-bytes N]\n"
          "  [--query-input FILE --ground-truth FILE --query-count N --query-offset N\n"
          "   --query-ef N --recall-k N --local-recall-out FILE]\n";
      return 0;
    }
    const ghnsw::PlacementResolver placement(cfg.owners, cfg.points_per_object);
    Metrics metrics;
    omp_set_num_threads(std::max(1u, cfg.threads));

    const double t0 = NowSec();
    std::cerr << "stage=load_vectors num_vectors=" << cfg.num_vectors << std::endl;
    const double t_load0 = NowSec();
    std::vector<std::string> raw_vectors;
    if (cfg.build_only && cfg.index_cache_mode == "load") {
      // Re-evaluating query ef on a frozen local graph does not require a
      // second full raw-vector copy. Input identity remains checked separately.
      ValidateVectorExtent(cfg);
    } else {
      raw_vectors = LoadVectors(cfg);
    }
    metrics.load_seconds = NowSec() - t_load0;
    WriteProgress(cfg, "load_vectors_done", cfg.num_vectors, cfg.num_vectors, NowSec() - t0);

    hnswlib::L2Space l2space(cfg.dim);
    hnswlib::InnerProductSpace ipspace(cfg.dim);
    hnswlib::SpaceInterface<float>* space =
        cfg.metric == ghnsw::kMetricIP
            ? static_cast<hnswlib::SpaceInterface<float>*>(&ipspace)
            : static_cast<hnswlib::SpaceInterface<float>*>(&l2space);
    std::unique_ptr<hnswlib::HierarchicalNSW<float>> index_ptr;
    const double t_build0 = NowSec();
    if (cfg.index_cache_mode == "load") {
      ValidateCacheIdentity(cfg);
      std::cerr << "stage=load_hnsw_cache path=" << cfg.index_cache << std::endl;
      const double start = NowSec();
      index_ptr = std::make_unique<hnswlib::HierarchicalNSW<float>>(
          space, cfg.index_cache, false, cfg.num_vectors, false);
      metrics.cache_load_seconds = NowSec() - start;
      metrics.index_cache_loaded = true;
    } else {
      std::cerr << "stage=convert_to_float" << std::endl;
      const double t_convert0 = NowSec();
      auto float_vectors =
          MakeFloatVectorsFromKind(raw_vectors, cfg.dim, cfg.vector_kind);
      metrics.convert_seconds = NowSec() - t_convert0;
      WriteProgress(
          cfg,
          "convert_to_float_done",
          cfg.num_vectors,
          cfg.num_vectors,
          NowSec() - t0);

      if (cfg.index_cache_mode == "build") {
        std::ifstream existing(cfg.index_cache, std::ios::binary);
        if (existing.good()) {
          throw std::runtime_error("refusing to overwrite HNSW index cache");
        }
      }
      std::cerr << "stage=build_hnsw threads=" << cfg.threads << std::endl;
      const double start = NowSec();
      index_ptr = std::make_unique<hnswlib::HierarchicalNSW<float>>(
          space, cfg.num_vectors, cfg.M, cfg.ef, cfg.seed, false);
#pragma omp parallel for schedule(dynamic, 256)
      for (uint64_t i = 0; i < cfg.num_vectors; ++i) {
        index_ptr->addPoint(
            float_vectors.data() + static_cast<size_t>(i) * cfg.dim, i);
      }
      metrics.hnsw_build_seconds = NowSec() - start;
      if (cfg.index_cache_mode == "build") {
        const double save_start = NowSec();
        index_ptr->saveIndex(cfg.index_cache);
        std::ifstream saved(cfg.index_cache, std::ios::binary | std::ios::ate);
        if (!saved || saved.tellg() <= 0) throw std::runtime_error("HNSW cache save failed");
        WriteCacheIdentity(cfg);
        metrics.cache_save_seconds = NowSec() - save_start;
      }
    }
    metrics.build_seconds = NowSec() - t_build0;
    auto& index = *index_ptr;
    if (index.cur_element_count != cfg.num_vectors) {
      throw std::runtime_error(
          "HNSW index cache element count does not match --num-vectors");
    }
    if (index.M_ != cfg.M) {
      throw std::runtime_error("HNSW index cache M does not match --M");
    }
    if (index.ef_construction_ != std::max(cfg.ef, cfg.M)) {
      throw std::runtime_error("HNSW index cache efConstruction does not match --ef");
    }
    if (index.data_size_ != static_cast<size_t>(cfg.dim) * sizeof(float)) {
      throw std::runtime_error("HNSW index cache dimension does not match --dim");
    }
    WriteProgress(
        cfg,
        metrics.index_cache_loaded ? "load_hnsw_cache_done" : "build_hnsw_done",
        cfg.num_vectors,
        cfg.num_vectors,
        NowSec() - t0);

    EvaluateLocalRecall(cfg, index);
    if (cfg.build_only) {
      metrics.total_seconds = NowSec() - t0;
      WriteMetrics(cfg, metrics);
      WriteProgress(cfg, "build_only_done", cfg.num_vectors, cfg.num_vectors, metrics.total_seconds);
      return 0;
    }

    std::cerr << "stage=map_internal_ids" << std::endl;
    const double t_mapping0 = NowSec();
    const hnswlib::tableint invalid_internal_id =
        std::numeric_limits<hnswlib::tableint>::max();
    std::vector<hnswlib::tableint> external_to_internal(
        cfg.num_vectors, invalid_internal_id);
    for (uint64_t internal = 0; internal < cfg.num_vectors; ++internal) {
      const uint64_t external = static_cast<uint64_t>(
          index.getExternalLabel(static_cast<hnswlib::tableint>(internal)));
      if (external >= cfg.num_vectors) {
        throw std::runtime_error(
            "HNSW external label is out of range: " + std::to_string(external));
      }
      if (external_to_internal[external] != invalid_internal_id) {
        throw std::runtime_error(
            "duplicate HNSW external label: " + std::to_string(external));
      }
      external_to_internal[external] = static_cast<hnswlib::tableint>(internal);
      if (external != internal) {
        metrics.permuted_internal_ids++;
      }
    }
    for (uint64_t external = 0; external < cfg.num_vectors; ++external) {
      if (external_to_internal[external] == invalid_internal_id) {
        throw std::runtime_error(
            "missing HNSW external label: " + std::to_string(external));
      }
    }
    metrics.id_mapping_seconds = NowSec() - t_mapping0;
    WriteProgress(cfg, "map_internal_ids_done", cfg.num_vectors, cfg.num_vectors, NowSec() - t0);

    librados::Rados cluster;
    Ensure(cluster.init2("client.admin", "client", 0), "rados init");
    Ensure(cluster.conf_read_file("/etc/ceph/ceph.conf"), "read ceph.conf");
    if (!cfg.keyring.empty()) {
      Ensure(cluster.conf_set("keyring", cfg.keyring.c_str()), "set keyring");
    }
    Ensure(cluster.conf_set("rados_osd_op_timeout", std::to_string(cfg.osd_op_timeout_seconds).c_str()), "set OSD operation timeout");
    Ensure(cluster.conf_set("rados_mon_op_timeout", "30"), "set monitor operation timeout");
    Ensure(cluster.connect(), "cluster connect");

    librados::IoCtx meta_ioctx;
    Ensure(cluster.ioctx_create(cfg.meta_pool.c_str(), meta_ioctx), "open meta pool");

    auto owner_pools = OwnerPools(cfg);
    std::vector<librados::IoCtx> owner_ioctxs(owner_pools.size());
    for (size_t i = 0; i < owner_pools.size(); ++i) {
      Ensure(cluster.ioctx_create(owner_pools[i].c_str(), owner_ioctxs[i]), "open pool " + owner_pools[i]);
      ceph::bufferlist empty;
      Ensure(owner_ioctxs[i].write_full(ghnsw::OwnerMetaOid(), empty), "reset owner meta object");
      metrics.object_reset_calls++;
    }

    std::vector<PersistStats> owner_stats(cfg.owners);
    metrics.owner_vector_seconds.resize(cfg.owners);
    metrics.owner_adjacency_seconds.resize(cfg.owners);
    std::atomic<uint64_t> vector_done{0}, adjacency_done{0};
    std::cerr << "stage=write_vector_payloads workers=" << cfg.import_workers << std::endl;
    const double t_vec0 = NowSec();
    nsvu::RunImportOwners(cfg.owners, cfg.import_workers,
        [&](uint32_t owner, const std::atomic<bool>& stopped) {
      const double owner_start = NowSec();
      auto& ioctx = owner_ioctxs[owner];
      auto* stats = &owner_stats[owner];
      OmapBatch refs(cfg, ioctx, stats), labels(cfg, ioctx, stats);
      labels.ResetObject(ghnsw::OwnerMetaOid());
      ceph::bufferlist payload;
      uint64_t current_chunk = UINT64_MAX, chunk_bytes = 0, batch_start = 0;
      nsvu::ForEachImportId(cfg.num_vectors, cfg.owners, owner, [&](uint64_t i) {
        if (stopped.load()) throw std::runtime_error("import cancelled");
        const auto location = placement.Resolve(i);
        if (location.shard_id != owner || placement.ResolveLabelShard(i) != owner) {
          throw std::runtime_error("import owner partition differs from placement");
        }
        const uint64_t chunk = location.chunk_id;
        if (current_chunk != chunk) {
          if (current_chunk != UINT64_MAX) {
            FlushPayloadBatch(ioctx, ghnsw::OwnerDataOid(current_chunk), batch_start, &payload, stats);
            refs.Flush();
          }
          current_chunk = chunk;
          chunk_bytes = batch_start = 0;
          ceph::bufferlist empty;
          Ensure(ioctx.write_full(ghnsw::OwnerDataOid(chunk), empty), "reset owner data object");
          stats->reset_calls++;
          refs.ResetObject(ghnsw::OwnerDataOid(chunk));
        }
        VectorRef ref;
        ref.global_id = i;
        ref.external_label = i;
        ref.offset = chunk_bytes;
        ref.bytes = static_cast<uint32_t>(raw_vectors[i].size());
        ref.dim = cfg.dim;
        ref.vector_kind = cfg.vector_kind;
        ref.flags = ghnsw::kVectorFlagActive;
        ref.level = static_cast<uint32_t>(index.element_levels_[external_to_internal[i]]);
        payload.append(raw_vectors[i]);
        chunk_bytes += raw_vectors[i].size();
        refs.Add(ghnsw::VecKey(i), EncodeMsg(ref));
        labels.Add(ghnsw::LabelKey(i), EncodeU64(i));
        if (payload.length() >= cfg.payload_batch_bytes) {
          FlushPayloadBatch(ioctx, ghnsw::OwnerDataOid(current_chunk), batch_start, &payload, stats);
          batch_start = chunk_bytes;
        }
        vector_done.fetch_add(1, std::memory_order_relaxed);
      });
      if (stopped.load()) return;
      if (current_chunk != UINT64_MAX) {
        FlushPayloadBatch(ioctx, ghnsw::OwnerDataOid(current_chunk), batch_start, &payload, stats);
        refs.Flush();
      }
      labels.Flush();
      metrics.owner_vector_seconds[owner] = NowSec() - owner_start;
    }, [&] {
      WriteProgress(cfg, "write_vector_payloads", vector_done.load(), cfg.num_vectors, NowSec() - t0);
    });
    if (vector_done.load() != cfg.num_vectors) throw std::runtime_error("incomplete vector import");
    metrics.vector_persist_seconds = NowSec() - t_vec0;
    WriteProgress(cfg, "write_vector_payloads_done", cfg.num_vectors, cfg.num_vectors, NowSec() - t0);

    // Joining the vector phase is a publication barrier: all raw payloads,
    // VectorRefs and label mappings exist before any adjacency phase begins.
    std::cerr << "stage=write_adjacency_omap workers=" << cfg.import_workers << std::endl;
    const double t_adj0 = NowSec();
    nsvu::RunImportOwners(cfg.owners, cfg.import_workers,
        [&](uint32_t owner, const std::atomic<bool>& stopped) {
      const double owner_start = NowSec();
      OmapBatch adjacency(cfg, owner_ioctxs[owner], &owner_stats[owner]);
      uint64_t current_chunk = UINT64_MAX;
      nsvu::ForEachImportId(cfg.num_vectors, cfg.owners, owner, [&](uint64_t i) {
        if (stopped.load()) throw std::runtime_error("import cancelled");
        const auto location = placement.Resolve(i);
        if (location.shard_id != owner) throw std::runtime_error("adjacency owner partition differs");
        if (current_chunk != location.chunk_id) {
          adjacency.ResetObject(ghnsw::OwnerDataOid(location.chunk_id));
          current_chunk = location.chunk_id;
        }
        adjacency.Add(ghnsw::NodeKey(i), EncodeMsg(BuildAdjacencyBlob(index, external_to_internal[i], i)));
        adjacency_done.fetch_add(1, std::memory_order_relaxed);
      });
      if (stopped.load()) return;
      adjacency.Flush();
      metrics.owner_adjacency_seconds[owner] = NowSec() - owner_start;
    }, [&] {
      WriteProgress(cfg, "write_adjacency_omap", adjacency_done.load(), cfg.num_vectors, NowSec() - t0);
    });
    if (adjacency_done.load() != cfg.num_vectors) throw std::runtime_error("incomplete adjacency import");
    metrics.adjacency_persist_seconds = NowSec() - t_adj0;
    for (const auto& stats : owner_stats) {
      metrics.omap_write_calls += stats.omap_calls;
      metrics.omap_application_bytes += stats.omap_bytes;
      metrics.payload_write_calls += stats.payload_calls;
      metrics.payload_write_bytes += stats.payload_bytes;
      metrics.object_reset_calls += stats.reset_calls;
      metrics.omap_rpc_seconds += stats.omap_seconds;
      metrics.payload_rpc_seconds += stats.payload_seconds;
    }
    WriteProgress(cfg, "write_adjacency_omap_done", cfg.num_vectors, cfg.num_vectors, NowSec() - t0);

    std::cerr << "stage=write_global_meta" << std::endl;
    const double t_meta0 = NowSec();
    librados::bufferlist empty;
    Ensure(meta_ioctx.write_full(cfg.meta_oid, empty), "write_full meta");
    metrics.object_reset_calls++;
    GlobalMeta meta;
    meta.enterpoint = static_cast<uint64_t>(
        index.getExternalLabel(index.enterpoint_node_));
    meta.max_level = static_cast<uint32_t>(std::max(0, index.maxlevel_));
    meta.cur_element_count = cfg.num_vectors;
    meta.next_global_id = cfg.num_vectors;
    meta.version = 1;
    meta.M = cfg.M;
    meta.ef = cfg.ef;
    meta.dim = cfg.dim;
    meta.vector_kind = cfg.vector_kind;
    meta.metric = cfg.metric;
    std::map<std::string, librados::bufferlist> meta_kv;
    meta_kv.emplace("meta/enterpoint", EncodeU64(meta.enterpoint));
    meta_kv.emplace("meta/max_level", EncodeU32(meta.max_level));
    meta_kv.emplace("meta/cur_element_count", EncodeU64(meta.cur_element_count));
    meta_kv.emplace("meta/next_global_id", EncodeU64(meta.next_global_id));
    meta_kv.emplace("meta/version", EncodeU64(meta.version));
    meta_kv.emplace("meta/M", EncodeU32(meta.M));
    meta_kv.emplace("meta/ef", EncodeU32(meta.ef));
    meta_kv.emplace("meta/dim", EncodeU32(meta.dim));
    meta_kv.emplace("meta/vector_kind", EncodeU32(meta.vector_kind));
    meta_kv.emplace("meta/metric", EncodeU32(meta.metric));
    const double meta_omap_start = NowSec();
    Ensure(meta_ioctx.omap_set(cfg.meta_oid, meta_kv), "omap_set meta");
    metrics.omap_rpc_seconds += NowSec() - meta_omap_start;
    metrics.omap_write_calls++;
    for (const auto& item : meta_kv) metrics.omap_application_bytes += item.first.size() + item.second.length();
    metrics.meta_persist_seconds = NowSec() - t_meta0;
    WriteProgress(cfg, "write_global_meta_done", cfg.num_vectors, cfg.num_vectors, NowSec() - t0);

    metrics.vectors_imported = cfg.num_vectors;
    metrics.total_seconds = NowSec() - t0;
    WriteMetrics(cfg, metrics);

    for (auto& ioctx : owner_ioctxs) {
      ioctx.close();
    }
    meta_ioctx.close();
    cluster.shutdown();
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << std::endl;
    return 1;
  }
}
