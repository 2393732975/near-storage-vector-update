#include <rados/librados.hpp>

#include "include/ceph_assert.h"

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "nsvu/protocol.hpp"

namespace {

using ghnsw::AdjacencyBlob;
using ghnsw::GlobalMeta;
using ghnsw::VectorRef;

struct Config {
  std::string keyring;
  std::string meta_pool = "nsvu_meta";
  std::string owner_pool_prefix = "nsvu_owner_";
  std::string meta_oid = "hnsw.global.meta";
  std::string output;
  uint32_t owners = 5;
  uint64_t points_per_object = 250000;
  uint32_t batch_size = 2048;
  uint32_t max_reported_errors = 100;
};

struct Report {
  uint64_t nodes_expected = 0;
  uint64_t nodes_checked = 0;
  uint64_t active_nodes = 0;
  uint64_t stale_nodes = 0;
  uint64_t edges_checked = 0;
  uint64_t cross_owner_edges = 0;
  uint64_t label_mappings = 0;
  uint64_t errors = 0;
  uint64_t warnings = 0;
  bool entrypoint_found = false;
  bool entrypoint_active = false;
  std::vector<std::string> error_samples;
  std::vector<std::string> warning_samples;
};

void Ensure(int result, const std::string& operation) {
  if (result < 0) {
    throw std::runtime_error(operation + " failed: " + std::to_string(result));
  }
}

template <typename T>
bool DecodeMessage(const ceph::bufferlist& value, T* decoded) {
  auto it = value.cbegin();
  try {
    decoded->decode(it);
    return true;
  } catch (const ceph::buffer::error&) {
    return false;
  }
}

template <typename T>
bool DecodeScalar(const ceph::bufferlist& value, T* decoded) {
  auto it = value.cbegin();
  try {
    ceph::decode(*decoded, it);
    return true;
  } catch (const ceph::buffer::error&) {
    return false;
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

void AddError(Report* report, const Config& cfg, const std::string& message) {
  report->errors++;
  if (report->error_samples.size() < cfg.max_reported_errors) {
    report->error_samples.push_back(message);
  }
}

void AddWarning(Report* report, const Config& cfg, const std::string& message) {
  report->warnings++;
  if (report->warning_samples.size() < cfg.max_reported_errors) {
    report->warning_samples.push_back(message);
  }
}

std::string JsonEscape(const std::string& value) {
  std::string escaped;
  escaped.reserve(value.size());
  for (char ch : value) {
    switch (ch) {
      case '\\': escaped += "\\\\"; break;
      case '"': escaped += "\\\""; break;
      case '\n': escaped += "\\n"; break;
      case '\r': escaped += "\\r"; break;
      case '\t': escaped += "\\t"; break;
      default: escaped += ch; break;
    }
  }
  return escaped;
}

GlobalMeta ReadGlobalMeta(librados::IoCtx& ioctx, const Config& cfg) {
  const std::set<std::string> keys = {
      "meta/enterpoint", "meta/max_level", "meta/cur_element_count",
      "meta/next_global_id", "meta/version", "meta/M", "meta/ef", "meta/dim",
      "meta/vector_kind", "meta/metric"};
  std::map<std::string, ceph::bufferlist> values;
  Ensure(ioctx.omap_get_vals_by_keys(cfg.meta_oid, keys, &values), "read global meta");

  auto require_u64 = [&](const std::string& key, uint64_t* value) {
    auto it = values.find(key);
    if (it == values.end() || !DecodeScalar(it->second, value)) {
      throw std::runtime_error("missing or invalid global meta key: " + key);
    }
  };
  auto require_u32 = [&](const std::string& key, uint32_t* value) {
    auto it = values.find(key);
    if (it == values.end() || !DecodeScalar(it->second, value)) {
      throw std::runtime_error("missing or invalid global meta key: " + key);
    }
  };

  GlobalMeta meta;
  require_u64("meta/enterpoint", &meta.enterpoint);
  require_u32("meta/max_level", &meta.max_level);
  require_u64("meta/cur_element_count", &meta.cur_element_count);
  require_u64("meta/next_global_id", &meta.next_global_id);
  require_u64("meta/version", &meta.version);
  require_u32("meta/M", &meta.M);
  require_u32("meta/ef", &meta.ef);
  require_u32("meta/dim", &meta.dim);
  auto kind = values.find("meta/vector_kind");
  if (kind != values.end() && !DecodeScalar(kind->second, &meta.vector_kind)) {
    throw std::runtime_error("invalid global meta key: meta/vector_kind");
  }
  auto metric = values.find("meta/metric");
  if (metric != values.end() && !DecodeScalar(metric->second, &meta.metric)) {
    throw std::runtime_error("invalid global meta key: meta/metric");
  }
  return meta;
}

void VerifyLabels(
    const Config& cfg,
    std::vector<librados::IoCtx>* owner_ioctxs,
    const std::vector<VectorRef>& active_refs,
    Report* report) {
  std::vector<std::set<std::string>> keys(cfg.owners);
  for (const auto& ref : active_refs) {
    const uint32_t owner = LabelOwnerFor(ref.external_label, cfg);
    keys[owner].insert(ghnsw::LabelKey(ref.external_label));
  }
  std::vector<std::map<std::string, ceph::bufferlist>> values(cfg.owners);
  for (uint32_t owner = 0; owner < cfg.owners; ++owner) {
    if (keys[owner].empty()) {
      continue;
    }
    Ensure(
        (*owner_ioctxs)[owner].omap_get_vals_by_keys(
            ghnsw::OwnerMetaOid(), keys[owner], &values[owner]),
        "read label batch");
  }
  for (const auto& ref : active_refs) {
    const uint32_t owner = LabelOwnerFor(ref.external_label, cfg);
    const std::string key = ghnsw::LabelKey(ref.external_label);
    auto it = values[owner].find(key);
    uint64_t actual_id = 0;
    if (it == values[owner].end()) {
      AddError(report, cfg, "active node " + std::to_string(ref.global_id) +
          " has no label mapping for " + std::to_string(ref.external_label));
    } else if (!DecodeScalar(it->second, &actual_id)) {
      AddError(report, cfg, "invalid label mapping for " +
          std::to_string(ref.external_label));
    } else if (actual_id != ref.global_id) {
      AddError(report, cfg, "active node " + std::to_string(ref.global_id) +
          " is not the current target of label " +
          std::to_string(ref.external_label) + " (target=" +
          std::to_string(actual_id) + ")");
    }
  }
}

void VerifyBatch(
    const Config& cfg,
    const GlobalMeta& meta,
    uint32_t owner,
    uint64_t chunk,
    uint64_t object_size,
    const std::vector<uint64_t>& ids,
    librados::IoCtx& ioctx,
    std::vector<librados::IoCtx>* owner_ioctxs,
    std::vector<uint8_t>* node_states,
    Report* report) {
  std::set<std::string> keys;
  for (uint64_t id : ids) {
    keys.insert(ghnsw::VecKey(id));
    keys.insert(ghnsw::NodeKey(id));
  }
  std::map<std::string, ceph::bufferlist> values;
  Ensure(
      ioctx.omap_get_vals_by_keys(ghnsw::OwnerDataOid(chunk), keys, &values),
      "read node batch");

  std::vector<VectorRef> active_refs;
  active_refs.reserve(ids.size());
  for (uint64_t id : ids) {
    VectorRef ref;
    AdjacencyBlob adjacency;
    const std::string vec_key = ghnsw::VecKey(id);
    const std::string node_key = ghnsw::NodeKey(id);
    auto vec_it = values.find(vec_key);
    auto node_it = values.find(node_key);
    if (vec_it == values.end()) {
      AddError(report, cfg, "missing VectorRef for node " + std::to_string(id));
      continue;
    }
    if (!DecodeMessage(vec_it->second, &ref)) {
      AddError(report, cfg, "invalid VectorRef for node " + std::to_string(id));
      continue;
    }
    report->nodes_checked++;
    if (ref.global_id != id) {
      AddError(report, cfg, "VectorRef key/id mismatch for node " + std::to_string(id));
    }
    if (OwnerFor(id, cfg) != owner || ChunkFor(id, cfg) != chunk) {
      AddError(report, cfg, "node routed to unexpected object: " + std::to_string(id));
    }
    const uint64_t element_bytes =
        meta.vector_kind == ghnsw::kVectorKindF32 ? sizeof(float) : sizeof(uint8_t);
    const uint64_t expected_bytes = static_cast<uint64_t>(meta.dim) * element_bytes;
    if (ref.dim != meta.dim || ref.vector_kind != meta.vector_kind || ref.bytes != expected_bytes) {
      AddError(report, cfg, "vector schema mismatch for node " + std::to_string(id));
    }
    if (ref.offset > object_size || ref.bytes > object_size - ref.offset) {
      AddError(report, cfg, "payload range exceeds object for node " + std::to_string(id));
    }
    if (ref.flags == ghnsw::kVectorFlagActive) {
      report->active_nodes++;
      (*node_states)[id] = ghnsw::kVectorFlagActive;
      active_refs.push_back(ref);
    } else if (ref.flags == ghnsw::kVectorFlagStale) {
      report->stale_nodes++;
      (*node_states)[id] = ghnsw::kVectorFlagStale;
    } else {
      AddError(report, cfg, "invalid flags for node " + std::to_string(id));
    }
    if (id == meta.enterpoint) {
      report->entrypoint_found = true;
      report->entrypoint_active = ref.flags == ghnsw::kVectorFlagActive;
    }

    if (node_it == values.end()) {
      AddError(report, cfg, "missing adjacency for node " + std::to_string(id));
      continue;
    }
    if (!DecodeMessage(node_it->second, &adjacency)) {
      AddError(report, cfg, "invalid adjacency for node " + std::to_string(id));
      continue;
    }
    if (adjacency.global_id != id || adjacency.level_count != adjacency.neighbors.size()) {
      AddError(report, cfg, "adjacency header mismatch for node " + std::to_string(id));
    }
    if (adjacency.neighbors.size() != static_cast<size_t>(ref.level) + 1) {
      AddError(report, cfg, "adjacency level count mismatch for node " + std::to_string(id));
    }
    for (size_t level = 0; level < adjacency.neighbors.size(); ++level) {
      const size_t degree_limit = level == 0 ? static_cast<size_t>(meta.M) * 2 : meta.M;
      const auto& neighbors = adjacency.neighbors[level];
      if (neighbors.size() > degree_limit) {
        AddError(report, cfg, "degree limit exceeded for node " + std::to_string(id) +
            " level " + std::to_string(level));
      }
      std::set<uint64_t> unique;
      for (uint64_t neighbor : neighbors) {
        report->edges_checked++;
        if (!unique.insert(neighbor).second) {
          AddError(report, cfg, "duplicate edge " + std::to_string(id) + " -> " +
              std::to_string(neighbor));
        }
        if (neighbor == id) {
          AddError(report, cfg, "self edge on node " + std::to_string(id));
        }
        if (neighbor >= meta.next_global_id) {
          AddError(report, cfg, "out-of-range edge " + std::to_string(id) + " -> " +
              std::to_string(neighbor));
        }
        if (OwnerFor(neighbor, cfg) != owner) {
          report->cross_owner_edges++;
        }
      }
    }
  }
  VerifyLabels(cfg, owner_ioctxs, active_refs, report);
}

void VerifyLabelTargets(
    const Config& cfg,
    std::vector<librados::IoCtx>* owner_ioctxs,
    const std::vector<uint8_t>& node_states,
    Report* report) {
  constexpr uint64_t kScanBatch = 4096;
  for (uint32_t owner = 0; owner < cfg.owners; ++owner) {
    std::string start_after;
    bool more = true;
    while (more) {
      std::map<std::string, ceph::bufferlist> values;
      Ensure(
          (*owner_ioctxs)[owner].omap_get_vals2(
              ghnsw::OwnerMetaOid(), start_after, "label/", kScanBatch, &values, &more),
          "scan label mappings");
      if (values.empty()) {
        if (more) {
          throw std::runtime_error("label scan returned no values with more=true");
        }
        break;
      }
      for (const auto& [key, value] : values) {
        report->label_mappings++;
        start_after = key;
        uint64_t label = 0;
        try {
          size_t parsed = 0;
          label = std::stoull(key.substr(6), &parsed);
          if (key.compare(0, 6, "label/") != 0 || parsed != key.size() - 6) {
            throw std::invalid_argument("invalid label key");
          }
        } catch (const std::exception&) {
          AddError(report, cfg, "invalid label key: " + key);
          continue;
        }
        if (LabelOwnerFor(label, cfg) != owner) {
          AddError(report, cfg, "label stored in wrong owner: " + key);
        }
        uint64_t target = 0;
        if (!DecodeScalar(value, &target)) {
          AddError(report, cfg, "invalid label target: " + key);
        } else if (target >= node_states.size()) {
          AddError(report, cfg, "label target is out of range: " + key + " -> " +
              std::to_string(target));
        } else if (node_states[target] != ghnsw::kVectorFlagActive) {
          AddError(report, cfg, "label target is not ACTIVE: " + key + " -> " +
              std::to_string(target));
        }
      }
    }
  }
}

Config ParseArgs(int argc, const char** argv) {
  Config cfg;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto next = [&](const char* option) -> std::string {
      if (i + 1 >= argc) {
        throw std::runtime_error(std::string("missing value for ") + option);
      }
      return argv[++i];
    };
    if (arg == "--keyring") {
      cfg.keyring = next("--keyring");
    } else if (arg == "--meta-pool") {
      cfg.meta_pool = next("--meta-pool");
    } else if (arg == "--owner-pool-prefix") {
      cfg.owner_pool_prefix = next("--owner-pool-prefix");
    } else if (arg == "--meta-oid") {
      cfg.meta_oid = next("--meta-oid");
    } else if (arg == "--owners") {
      cfg.owners = static_cast<uint32_t>(std::stoul(next("--owners")));
    } else if (arg == "--points-per-object") {
      cfg.points_per_object = std::stoull(next("--points-per-object"));
    } else if (arg == "--batch-size") {
      cfg.batch_size = static_cast<uint32_t>(std::stoul(next("--batch-size")));
    } else if (arg == "--max-reported-errors") {
      cfg.max_reported_errors =
          static_cast<uint32_t>(std::stoul(next("--max-reported-errors")));
    } else if (arg == "--output") {
      cfg.output = next("--output");
    } else {
      throw std::runtime_error("unknown option: " + arg);
    }
  }
  if (cfg.owners == 0 || cfg.points_per_object == 0 || cfg.batch_size == 0) {
    throw std::runtime_error("owners, points-per-object and batch-size must be positive");
  }
  return cfg;
}

void WriteStringArray(
    std::ostream& out,
    const std::string& name,
    const std::vector<std::string>& values,
    bool trailing_comma) {
  out << "  \"" << name << "\": [";
  for (size_t i = 0; i < values.size(); ++i) {
    if (i != 0) {
      out << ", ";
    }
    out << "\"" << JsonEscape(values[i]) << "\"";
  }
  out << "]" << (trailing_comma ? "," : "") << "\n";
}

void WriteReport(const Config& cfg, const GlobalMeta& meta, const Report& report) {
  std::ofstream file;
  std::ostream* output = &std::cout;
  if (!cfg.output.empty()) {
    file.open(cfg.output);
    if (!file) {
      throw std::runtime_error("failed to open report: " + cfg.output);
    }
    output = &file;
  }
  std::ostream& out = *output;
  out << "{\n";
  out << "  \"status\": \"" << (report.errors == 0 ? "pass" : "fail") << "\",\n";
  out << "  \"global_meta\": {\n";
  out << "    \"enterpoint\": " << meta.enterpoint << ",\n";
  out << "    \"max_level\": " << meta.max_level << ",\n";
  out << "    \"cur_element_count\": " << meta.cur_element_count << ",\n";
  out << "    \"next_global_id\": " << meta.next_global_id << ",\n";
  out << "    \"version\": " << meta.version << ",\n";
  out << "    \"M\": " << meta.M << ",\n";
  out << "    \"ef\": " << meta.ef << ",\n";
  out << "    \"dim\": " << meta.dim << "\n";
  out << "  },\n";
  out << "  \"nodes_expected\": " << report.nodes_expected << ",\n";
  out << "  \"nodes_checked\": " << report.nodes_checked << ",\n";
  out << "  \"active_nodes\": " << report.active_nodes << ",\n";
  out << "  \"stale_nodes\": " << report.stale_nodes << ",\n";
  out << "  \"edges_checked\": " << report.edges_checked << ",\n";
  out << "  \"cross_owner_edges\": " << report.cross_owner_edges << ",\n";
  out << "  \"label_mappings\": " << report.label_mappings << ",\n";
  out << "  \"cross_owner_edge_ratio\": "
      << (report.edges_checked > 0
              ? static_cast<double>(report.cross_owner_edges) /
                    static_cast<double>(report.edges_checked)
              : 0.0)
      << ",\n";
  out << "  \"entrypoint_found\": " << (report.entrypoint_found ? "true" : "false") << ",\n";
  out << "  \"entrypoint_active\": " << (report.entrypoint_active ? "true" : "false") << ",\n";
  out << "  \"errors\": " << report.errors << ",\n";
  out << "  \"warnings\": " << report.warnings << ",\n";
  WriteStringArray(out, "error_samples", report.error_samples, true);
  WriteStringArray(out, "warning_samples", report.warning_samples, false);
  out << "}\n";
}

int Run(const Config& cfg) {
  librados::Rados cluster;
  Ensure(cluster.init2("client.admin", "client", 0), "rados init");
  Ensure(cluster.conf_read_file("/etc/ceph/ceph.conf"), "read ceph.conf");
  if (!cfg.keyring.empty()) {
    Ensure(cluster.conf_set("keyring", cfg.keyring.c_str()), "set keyring");
  }
  Ensure(cluster.connect(), "cluster connect");

  librados::IoCtx meta_ioctx;
  Ensure(cluster.ioctx_create(cfg.meta_pool.c_str(), meta_ioctx), "open meta pool");
  std::vector<librados::IoCtx> owner_ioctxs(cfg.owners);
  for (uint32_t owner = 0; owner < cfg.owners; ++owner) {
    Ensure(
        cluster.ioctx_create(
            (cfg.owner_pool_prefix + std::to_string(owner)).c_str(),
            owner_ioctxs[owner]),
        "open owner pool");
  }

  const GlobalMeta meta = ReadGlobalMeta(meta_ioctx, cfg);
  Report report;
  report.nodes_expected = meta.next_global_id;
  std::vector<uint8_t> node_states(meta.next_global_id, 0);
  for (uint32_t owner = 0; owner < cfg.owners; ++owner) {
    const uint64_t local_count =
        meta.next_global_id > owner
            ? (meta.next_global_id - 1 - owner) / cfg.owners + 1
            : 0;
    const uint64_t chunks =
        (local_count + cfg.points_per_object - 1) / cfg.points_per_object;
    for (uint64_t chunk = 0; chunk < chunks; ++chunk) {
      const std::string oid = ghnsw::OwnerDataOid(chunk);
      uint64_t object_size = 0;
      time_t modified = 0;
      const int stat_result = owner_ioctxs[owner].stat(oid, &object_size, &modified);
      if (stat_result < 0) {
        AddError(&report, cfg, "missing data object " +
            std::to_string(owner) + ":" + std::to_string(chunk));
        continue;
      }
      const uint64_t local_begin = chunk * cfg.points_per_object;
      const uint64_t local_end = std::min(local_count, local_begin + cfg.points_per_object);
      for (uint64_t cursor = local_begin; cursor < local_end; cursor += cfg.batch_size) {
        const uint64_t end = std::min(local_end, cursor + cfg.batch_size);
        std::vector<uint64_t> ids;
        ids.reserve(static_cast<size_t>(end - cursor));
        for (uint64_t local = cursor; local < end; ++local) {
          ids.push_back(local * cfg.owners + owner);
        }
        VerifyBatch(
            cfg,
            meta,
            owner,
            chunk,
            object_size,
            ids,
            owner_ioctxs[owner],
            &owner_ioctxs,
            &node_states,
            &report);
        if (report.nodes_checked > 0 && report.nodes_checked % 1000000 < cfg.batch_size) {
          std::cerr << "checked " << report.nodes_checked << "/" << meta.next_global_id
                    << " nodes" << std::endl;
        }
      }
    }
  }

  VerifyLabelTargets(cfg, &owner_ioctxs, node_states, &report);

  if (report.nodes_checked != meta.cur_element_count) {
    AddError(&report, cfg, "node count does not match cur_element_count: checked=" +
        std::to_string(report.nodes_checked) + " meta=" +
        std::to_string(meta.cur_element_count));
  }
  if (meta.next_global_id > 0 && !report.entrypoint_found) {
    AddError(&report, cfg, "global entrypoint does not exist");
  } else if (report.entrypoint_found && !report.entrypoint_active) {
    AddError(&report, cfg, "global entrypoint is stale");
  }
  WriteReport(cfg, meta, report);

  for (auto& ioctx : owner_ioctxs) {
    ioctx.close();
  }
  meta_ioctx.close();
  cluster.shutdown();
  return report.errors == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, const char** argv) {
  try {
    return Run(ParseArgs(argc, argv));
  } catch (const std::exception& error) {
    std::cerr << "error: " << error.what() << std::endl;
    return 2;
  }
}
