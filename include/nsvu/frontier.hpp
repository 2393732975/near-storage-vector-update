#pragma once

#include <cstddef>
#include <cstdint>
#include <cmath>
#include <set>
#include <stdexcept>
#include <utility>
#include <vector>

namespace nsvu {

template <typename Queue>
std::vector<typename Queue::value_type> SelectFrontierWindow(
    Queue* candidates,
    float lower_bound,
    size_t max_nodes) {
  if (max_nodes == 0) {
    throw std::invalid_argument("frontier window must be positive");
  }

  std::vector<typename Queue::value_type> selected;
  selected.reserve(max_nodes);
  while (selected.size() < max_nodes && !candidates->empty()) {
    const auto candidate = candidates->top();
    if (candidate.dist > lower_bound) {
      break;
    }
    candidates->pop();
    selected.push_back(candidate);
  }
  return selected;
}

struct FrontierCandidateRef {
  uint64_t id = 0;
  uint32_t shard_id = 0;
  uint64_t chunk_id = 0;
};

struct AdaptiveFrontierDecision {
  size_t accepted_nodes = 1;
  uint64_t first_candidates = 0;
  uint64_t incremental_candidates = 0;
  uint64_t sequential_batches = 0;
  uint64_t merged_batches = 0;
  uint64_t predicted_batches_saved = 0;
  bool saw_batch_gain = false;
  bool budget_limited = false;
};

inline bool FrontierHeadWithinSearchBound(float head_distance, float lower_bound) {
  return head_distance <= lower_bound;
}

inline AdaptiveFrontierDecision DecideAdaptiveFrontierPrefix(
    const std::vector<std::vector<FrontierCandidateRef>>& candidates_by_node,
    const std::set<uint64_t>& visited,
    uint64_t prior_base_candidates,
    uint64_t prior_incremental_candidates,
    double candidate_budget_ratio,
    double minimum_batch_savings_ratio) {
  if (candidates_by_node.empty()) {
    throw std::invalid_argument("adaptive frontier requires a window head");
  }
  if (candidate_budget_ratio < 0.0 || minimum_batch_savings_ratio < 0.0 ||
      minimum_batch_savings_ratio > 1.0) {
    throw std::invalid_argument("invalid adaptive frontier ratio");
  }

  AdaptiveFrontierDecision decision;
  std::set<uint64_t> seen = visited;
  std::set<std::pair<uint32_t, uint64_t>> merged_objects;
  uint64_t merged_candidates = 0;
  uint64_t sequential_batches = 0;

  for (size_t index = 0; index < candidates_by_node.size(); ++index) {
    std::set<std::pair<uint32_t, uint64_t>> node_objects;
    for (const auto& candidate : candidates_by_node[index]) {
      if (!seen.insert(candidate.id).second) {
        continue;
      }
      ++merged_candidates;
      const auto object = std::make_pair(candidate.shard_id, candidate.chunk_id);
      node_objects.insert(object);
      merged_objects.insert(object);
    }
    sequential_batches += node_objects.size();
    if (index == 0) {
      decision.first_candidates = merged_candidates;
      decision.sequential_batches = sequential_batches;
      decision.merged_batches = merged_objects.size();
      continue;
    }

    const uint64_t saved = sequential_batches - merged_objects.size();
    const double savings_ratio = sequential_batches > 0
        ? static_cast<double>(saved) / static_cast<double>(sequential_batches)
        : 0.0;
    if (saved == 0 || savings_ratio < minimum_batch_savings_ratio) {
      continue;
    }
    decision.saw_batch_gain = true;

    const uint64_t incremental = merged_candidates - decision.first_candidates;
    const uint64_t base_total = prior_base_candidates + decision.first_candidates;
    const uint64_t allowed_incremental = static_cast<uint64_t>(
        std::floor(candidate_budget_ratio * static_cast<double>(base_total)));
    if (prior_incremental_candidates + incremental > allowed_incremental) {
      decision.budget_limited = true;
      continue;
    }

    decision.accepted_nodes = index + 1;
    decision.incremental_candidates = incremental;
    decision.sequential_batches = sequential_batches;
    decision.merged_batches = merged_objects.size();
    decision.predicted_batches_saved = saved;
  }
  return decision;
}

}  // namespace nsvu
