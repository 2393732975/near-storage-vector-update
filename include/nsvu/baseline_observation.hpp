#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <set>
#include <vector>

namespace ghnsw {

struct ActiveQueryCandidate {
  uint64_t global_id = 0;
  uint64_t external_label = 0;
  uint64_t current_global_id = UINT64_MAX;
  float distance = 0.0f;
  bool active = false;
};

// A stale point may be traversed by search, but cannot become a result. The
// current label mapping is authoritative even if an old ref still says ACTIVE.
inline std::vector<ActiveQueryCandidate> SelectActiveQueryResults(
    std::vector<ActiveQueryCandidate> candidates, size_t k) {
  candidates.erase(std::remove_if(candidates.begin(), candidates.end(), [](const auto& candidate) {
    return !candidate.active || !std::isfinite(candidate.distance) ||
        candidate.global_id != candidate.current_global_id;
  }), candidates.end());
  std::sort(candidates.begin(), candidates.end(), [](const auto& a, const auto& b) {
    if (a.distance != b.distance) return a.distance < b.distance;
    return a.external_label < b.external_label;
  });
  std::set<uint64_t> labels;
  std::vector<ActiveQueryCandidate> result;
  for (const auto& candidate : candidates) {
    if (result.size() == k) break;
    if (!labels.insert(candidate.external_label).second) continue;
    result.push_back(candidate);
  }
  return result;
}

inline uint64_t RpcLatencyBucketUs(double seconds) {
  const double micros = std::max(0.0, seconds) * 1000000.0;
  uint64_t bucket = 1;
  while (bucket < (UINT64_MAX >> 1) && static_cast<double>(bucket) < micros) {
    bucket <<= 1;
  }
  return bucket;
}

}  // namespace ghnsw
