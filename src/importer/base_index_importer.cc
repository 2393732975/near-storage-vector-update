#include <rados/librados.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <cstring>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include <omp.h>

#include "include/ceph_assert.h"
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
  std::string meta_pool = "ghnsw_meta";
  std::string owner_pool_prefix = "ghnsw_owner_";
  std::string data_oid = "hnsw.owner.data";
  std::string meta_oid = "hnsw.global.meta";
  std::string metrics_out;
  std::string progress_out;
  uint64_t num_vectors = 1000000;
  uint32_t dim = 128;
  uint32_t owners = 5;
  uint64_t points_per_object = 250000;
  uint32_t M = 8;
  uint32_t ef = 32;
  uint32_t threads = 32;
  uint32_t omap_batch = 2048;
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
  double vector_persist_seconds = 0.0;
  double adjacency_persist_seconds = 0.0;
  double meta_persist_seconds = 0.0;
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
  std::ofstream out(cfg.progress_out);
  out << "{\n";
  out << "  \"mode\": \"import_base\",\n";
  out << "  \"stage\": \"" << stage << "\",\n";
  out << "  \"processed\": " << processed << ",\n";
  out << "  \"total\": " << total << ",\n";
  out << "  \"elapsed_seconds\": " << elapsed_seconds << "\n";
  out << "}\n";
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

uint32_t OwnerFor(uint64_t global_id, const Config& cfg) {
  return static_cast<uint32_t>(global_id % cfg.owners);
}

uint32_t LabelOwnerFor(uint64_t external_label, const Config& cfg) {
  return static_cast<uint32_t>(external_label % cfg.owners);
}

uint64_t ChunkFor(uint64_t global_id, const Config& cfg) {
  return (global_id / cfg.owners) / cfg.points_per_object;
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

std::vector<std::string> LoadVectors(const Config& cfg) {
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
    if (cfg.num_vectors > n) {
      throw std::runtime_error("requested vectors exceed fbin count");
    }
  } else if (cfg.input_format != "u8bin") {
    throw std::runtime_error("unsupported input format: " + cfg.input_format);
  }
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
    uint64_t global_id) {
  AdjacencyBlob blob;
  blob.global_id = global_id;
  blob.level_count = static_cast<uint32_t>(index.element_levels_[global_id] + 1);
  blob.neighbors.resize(blob.level_count);
  for (uint32_t level = 0; level < blob.level_count; ++level) {
    auto* ll = level == 0 ? index.get_linklist0(global_id)
                          : index.get_linklist(global_id, static_cast<int>(level));
    const uint32_t neighbor_count = *ll;
    auto* neighbors = reinterpret_cast<hnswlib::tableint*>(ll + 1);
    auto& out = blob.neighbors[level];
    out.reserve(neighbor_count);
    for (uint32_t i = 0; i < neighbor_count; ++i) {
      out.push_back(static_cast<uint64_t>(neighbors[i]));
    }
  }
  return blob;
}

void FlushOmapBatch(
    librados::IoCtx& ioctx,
    const std::string& oid,
    std::map<std::string, librados::bufferlist>* kv) {
  if (kv->empty()) {
    return;
  }
  Ensure(ioctx.omap_set(oid, *kv), "omap_set " + oid);
  kv->clear();
}

void FlushPayloadBatch(
    librados::IoCtx& ioctx,
    const std::string& oid,
    uint64_t offset,
    ceph::bufferlist* bl) {
  if (bl->length() == 0) {
    return;
  }
  Ensure(ioctx.write(oid, *bl, bl->length(), offset), "write payload chunk " + oid);
  bl->clear();
}

void WriteMetrics(const Config& cfg, const Metrics& metrics) {
  if (cfg.metrics_out.empty()) {
    return;
  }
  std::ofstream out(cfg.metrics_out);
  out << "{\n";
  out << "  \"mode\": \"import_base\",\n";
  out << "  \"vectors_imported\": " << metrics.vectors_imported << ",\n";
  out << "  \"load_seconds\": " << metrics.load_seconds << ",\n";
  out << "  \"convert_seconds\": " << metrics.convert_seconds << ",\n";
  out << "  \"build_seconds\": " << metrics.build_seconds << ",\n";
  out << "  \"vector_persist_seconds\": " << metrics.vector_persist_seconds << ",\n";
  out << "  \"adjacency_persist_seconds\": " << metrics.adjacency_persist_seconds << ",\n";
  out << "  \"meta_persist_seconds\": " << metrics.meta_persist_seconds << ",\n";
  out << "  \"total_seconds\": " << metrics.total_seconds << "\n";
  out << "}\n";
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
    if (arg == "--input") {
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
    } else if (arg == "--num-vectors") {
      cfg.num_vectors = std::stoull(next("--num-vectors"));
    } else if (arg == "--dim") {
      cfg.dim = static_cast<uint32_t>(std::stoul(next("--dim")));
    } else if (arg == "--owners") {
      cfg.owners = static_cast<uint32_t>(std::stoul(next("--owners")));
    } else if (arg == "--points-per-object") {
      cfg.points_per_object = std::stoull(next("--points-per-object"));
    } else if (arg == "--M") {
      cfg.M = static_cast<uint32_t>(std::stoul(next("--M")));
    } else if (arg == "--ef") {
      cfg.ef = static_cast<uint32_t>(std::stoul(next("--ef")));
    } else if (arg == "--threads") {
      cfg.threads = static_cast<uint32_t>(std::stoul(next("--threads")));
    } else if (arg == "--omap-batch") {
      cfg.omap_batch = static_cast<uint32_t>(std::stoul(next("--omap-batch")));
    } else if (arg == "--payload-batch-bytes") {
      cfg.payload_batch_bytes = std::stoull(next("--payload-batch-bytes"));
    } else if (arg == "--vector-kind") {
      cfg.vector_kind = parse_vector_kind(next("--vector-kind"));
    } else if (arg == "--metric") {
      cfg.metric = parse_metric(next("--metric"));
    }
  }
  return cfg;
}

}  // namespace

int main(int argc, const char** argv) {
  try {
    Config cfg = ParseArgs(argc, argv);
    Metrics metrics;
    omp_set_num_threads(std::max(1u, cfg.threads));

    const double t0 = NowSec();
    std::cerr << "stage=load_vectors num_vectors=" << cfg.num_vectors << std::endl;
    const double t_load0 = NowSec();
    auto raw_vectors = LoadVectors(cfg);
    metrics.load_seconds = NowSec() - t_load0;
    WriteProgress(cfg, "load_vectors_done", cfg.num_vectors, cfg.num_vectors, NowSec() - t0);

    std::cerr << "stage=convert_to_float" << std::endl;
    const double t_convert0 = NowSec();
    auto float_vectors = MakeFloatVectorsFromKind(raw_vectors, cfg.dim, cfg.vector_kind);
    metrics.convert_seconds = NowSec() - t_convert0;
    WriteProgress(cfg, "convert_to_float_done", cfg.num_vectors, cfg.num_vectors, NowSec() - t0);

    std::cerr << "stage=build_hnsw threads=" << cfg.threads << std::endl;
    const double t_build0 = NowSec();
    hnswlib::L2Space l2space(cfg.dim);
    hnswlib::InnerProductSpace ipspace(cfg.dim);
    hnswlib::SpaceInterface<float>* space =
        cfg.metric == ghnsw::kMetricIP
            ? static_cast<hnswlib::SpaceInterface<float>*>(&ipspace)
            : static_cast<hnswlib::SpaceInterface<float>*>(&l2space);
    hnswlib::HierarchicalNSW<float> index(
        space, cfg.num_vectors, cfg.M, cfg.ef, 100, false);
#pragma omp parallel for schedule(dynamic, 256)
    for (uint64_t i = 0; i < cfg.num_vectors; ++i) {
      index.addPoint(float_vectors.data() + static_cast<size_t>(i) * cfg.dim, i);
    }
    metrics.build_seconds = NowSec() - t_build0;
    WriteProgress(cfg, "build_hnsw_done", cfg.num_vectors, cfg.num_vectors, NowSec() - t0);

    librados::Rados cluster;
    Ensure(cluster.init2("client.admin", "client", 0), "rados init");
    Ensure(cluster.conf_read_file("/etc/ceph/ceph.conf"), "read ceph.conf");
    if (!cfg.keyring.empty()) {
      Ensure(cluster.conf_set("keyring", cfg.keyring.c_str()), "set keyring");
    }
    Ensure(cluster.connect(), "cluster connect");

    librados::IoCtx meta_ioctx;
    Ensure(cluster.ioctx_create(cfg.meta_pool.c_str(), meta_ioctx), "open meta pool");

    auto owner_pools = OwnerPools(cfg);
    std::vector<librados::IoCtx> owner_ioctxs(owner_pools.size());
    for (size_t i = 0; i < owner_pools.size(); ++i) {
      Ensure(cluster.ioctx_create(owner_pools[i].c_str(), owner_ioctxs[i]), "open pool " + owner_pools[i]);
      ceph::bufferlist empty;
      Ensure(owner_ioctxs[i].write_full(ghnsw::OwnerMetaOid(), empty), "reset owner meta object");
    }

    std::cerr << "stage=write_vector_payloads" << std::endl;
    const double t_vec0 = NowSec();
    std::vector<ceph::bufferlist> owner_payload_batches(cfg.owners);
    std::vector<std::map<std::string, librados::bufferlist>> owner_data_meta_batches(cfg.owners);
    std::vector<std::map<std::string, librados::bufferlist>> owner_label_batches(cfg.owners);
    std::vector<uint64_t> owner_current_chunk(cfg.owners, UINT64_MAX);
    std::vector<uint64_t> owner_chunk_bytes(cfg.owners, 0);
    std::vector<uint64_t> owner_batch_start(cfg.owners, 0);
    auto flush_owner_data_meta = [&](uint32_t owner) {
      if (owner_current_chunk[owner] == UINT64_MAX) {
        owner_data_meta_batches[owner].clear();
        return;
      }
      FlushOmapBatch(
          owner_ioctxs[owner],
          ghnsw::OwnerDataOid(owner_current_chunk[owner]),
          &owner_data_meta_batches[owner]);
    };
    auto flush_owner_labels = [&](uint32_t owner) {
      FlushOmapBatch(owner_ioctxs[owner], ghnsw::OwnerMetaOid(), &owner_label_batches[owner]);
    };
    for (uint64_t i = 0; i < cfg.num_vectors; ++i) {
      const uint32_t owner = OwnerFor(i, cfg);
      const uint64_t chunk = ChunkFor(i, cfg);
      if (owner_current_chunk[owner] != chunk) {
        if (owner_current_chunk[owner] != UINT64_MAX) {
          FlushPayloadBatch(
              owner_ioctxs[owner],
              ghnsw::OwnerDataOid(owner_current_chunk[owner]),
              owner_batch_start[owner],
              &owner_payload_batches[owner]);
          flush_owner_data_meta(owner);
        }
        owner_current_chunk[owner] = chunk;
        owner_chunk_bytes[owner] = 0;
        owner_batch_start[owner] = 0;
        ceph::bufferlist empty;
        Ensure(
            owner_ioctxs[owner].write_full(ghnsw::OwnerDataOid(chunk), empty),
            "reset owner data object");
      }
      VectorRef ref;
      ref.global_id = i;
      ref.external_label = i;
      ref.offset = owner_chunk_bytes[owner];
      ref.bytes = static_cast<uint32_t>(raw_vectors[i].size());
      ref.dim = cfg.dim;
      ref.vector_kind = cfg.vector_kind;
      ref.flags = ghnsw::kVectorFlagActive;
      ref.level = static_cast<uint32_t>(index.element_levels_[i]);
      owner_payload_batches[owner].append(raw_vectors[i]);
      owner_chunk_bytes[owner] += raw_vectors[i].size();
      owner_data_meta_batches[owner].emplace(ghnsw::VecKey(i), EncodeMsg(ref));
      const uint32_t label_owner = LabelOwnerFor(i, cfg);
      owner_label_batches[label_owner].emplace(ghnsw::LabelKey(i), EncodeU64(i));
      if (owner_data_meta_batches[owner].size() >= static_cast<size_t>(cfg.omap_batch)) {
        flush_owner_data_meta(owner);
      }
      if (owner_label_batches[label_owner].size() >= static_cast<size_t>(cfg.omap_batch)) {
        flush_owner_labels(label_owner);
      }
      if (owner_payload_batches[owner].length() >= cfg.payload_batch_bytes) {
        FlushPayloadBatch(
            owner_ioctxs[owner],
            ghnsw::OwnerDataOid(owner_current_chunk[owner]),
            owner_batch_start[owner],
            &owner_payload_batches[owner]);
        owner_batch_start[owner] = owner_chunk_bytes[owner];
      }
      if ((i + 1) % 1000000 == 0) {
        WriteProgress(cfg, "write_vector_payloads", i + 1, cfg.num_vectors, NowSec() - t0);
      }
    }
    for (uint32_t owner = 0; owner < cfg.owners; ++owner) {
      if (owner_current_chunk[owner] != UINT64_MAX) {
        FlushPayloadBatch(
            owner_ioctxs[owner],
            ghnsw::OwnerDataOid(owner_current_chunk[owner]),
            owner_batch_start[owner],
            &owner_payload_batches[owner]);
        flush_owner_data_meta(owner);
      }
      flush_owner_labels(owner);
    }
    metrics.vector_persist_seconds = NowSec() - t_vec0;
    WriteProgress(cfg, "write_vector_payloads_done", cfg.num_vectors, cfg.num_vectors, NowSec() - t0);

    std::cerr << "stage=write_adjacency_omap" << std::endl;
    const double t_adj0 = NowSec();
    std::vector<std::map<std::string, librados::bufferlist>> owner_adj_batches(cfg.owners);
    std::vector<uint64_t> owner_adj_chunk(cfg.owners, UINT64_MAX);
    for (uint64_t i = 0; i < cfg.num_vectors; ++i) {
      const uint32_t owner = OwnerFor(i, cfg);
      const uint64_t chunk = ChunkFor(i, cfg);
      if (owner_adj_chunk[owner] != chunk) {
        if (owner_adj_chunk[owner] != UINT64_MAX) {
          FlushOmapBatch(
              owner_ioctxs[owner],
              ghnsw::OwnerDataOid(owner_adj_chunk[owner]),
              &owner_adj_batches[owner]);
        }
        owner_adj_chunk[owner] = chunk;
      }
      AdjacencyBlob adj = BuildAdjacencyBlob(index, i);
      owner_adj_batches[owner].emplace(ghnsw::NodeKey(i), EncodeMsg(adj));
      if (owner_adj_batches[owner].size() >= cfg.omap_batch) {
        FlushOmapBatch(
            owner_ioctxs[owner],
            ghnsw::OwnerDataOid(owner_adj_chunk[owner]),
            &owner_adj_batches[owner]);
      }
      if ((i + 1) % 1000000 == 0) {
        WriteProgress(cfg, "write_adjacency_omap", i + 1, cfg.num_vectors, NowSec() - t0);
      }
    }
    for (uint32_t owner = 0; owner < cfg.owners; ++owner) {
      if (owner_adj_chunk[owner] != UINT64_MAX) {
        FlushOmapBatch(
            owner_ioctxs[owner],
            ghnsw::OwnerDataOid(owner_adj_chunk[owner]),
            &owner_adj_batches[owner]);
      }
    }
    metrics.adjacency_persist_seconds = NowSec() - t_adj0;
    WriteProgress(cfg, "write_adjacency_omap_done", cfg.num_vectors, cfg.num_vectors, NowSec() - t0);

    std::cerr << "stage=write_global_meta" << std::endl;
    const double t_meta0 = NowSec();
    librados::bufferlist empty;
    Ensure(meta_ioctx.write_full(cfg.meta_oid, empty), "write_full meta");
    GlobalMeta meta;
    meta.enterpoint = static_cast<uint64_t>(index.enterpoint_node_);
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
    Ensure(meta_ioctx.omap_set(cfg.meta_oid, meta_kv), "omap_set meta");
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
