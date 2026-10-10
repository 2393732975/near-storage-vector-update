#include <cassert>
#include <cstdint>
#include <functional>
#include <queue>
#include <stdexcept>
#include <vector>

#include "nsvu/frontier.hpp"

namespace {

struct Candidate {
  uint64_t id = 0;
  float dist = 0.0f;
};

struct MinDistance {
  bool operator()(const Candidate& a, const Candidate& b) const {
    return a.dist > b.dist;
  }
};

using Queue = std::priority_queue<Candidate, std::vector<Candidate>, MinDistance>;

Queue MakeQueue() {
  Queue queue;
  queue.push({4, 4.0f});
  queue.push({1, 1.0f});
  queue.push({3, 3.0f});
  queue.push({2, 2.0f});
  return queue;
}

void TestWidthOnePreservesSerialSelection() {
  Queue queue = MakeQueue();
  const auto selected = nsvu::SelectFrontierWindow(&queue, 10.0f, 1);
  assert(selected.size() == 1);
  assert(selected[0].id == 1);
  assert(queue.size() == 3);
  assert(queue.top().id == 2);
}

void TestWindowStopsAtSearchLowerBound() {
  Queue queue = MakeQueue();
  const auto selected = nsvu::SelectFrontierWindow(&queue, 2.5f, 4);
  assert(selected.size() == 2);
  assert(selected[0].id == 1);
  assert(selected[1].id == 2);
  assert(queue.size() == 2);
  assert(queue.top().id == 3);
}

void TestWindowHonorsConfiguredWidth() {
  Queue queue = MakeQueue();
  const auto selected = nsvu::SelectFrontierWindow(&queue, 10.0f, 3);
  assert(selected.size() == 3);
  assert(selected[0].id == 1);
  assert(selected[1].id == 2);
  assert(selected[2].id == 3);
  assert(queue.size() == 1);
  assert(queue.top().id == 4);
}

void TestZeroWidthIsRejected() {
  Queue queue = MakeQueue();
  bool rejected = false;
  try {
    (void)nsvu::SelectFrontierWindow(&queue, 10.0f, 0);
  } catch (const std::invalid_argument&) {
    rejected = true;
  }
  assert(rejected);
  assert(queue.size() == 4);
}

void TestAdaptivePrefixAcceptsObjectReuseWithinBudget() {
  using Ref = nsvu::FrontierCandidateRef;
  const std::vector<std::vector<Ref>> candidates = {
      {{1, 0, 0}, {2, 1, 0}},
      {{3, 0, 0}, {4, 1, 0}},
  };
  const auto decision = nsvu::DecideAdaptiveFrontierPrefix(
      candidates, {}, 10, 0, 0.20, 0.25);
  assert(decision.accepted_nodes == 2);
  assert(decision.first_candidates == 2);
  assert(decision.incremental_candidates == 2);
  assert(decision.sequential_batches == 4);
  assert(decision.merged_batches == 2);
  assert(decision.predicted_batches_saved == 2);
}

void TestAdaptivePrefixRejectsNoObjectGain() {
  using Ref = nsvu::FrontierCandidateRef;
  const std::vector<std::vector<Ref>> candidates = {
      {{1, 0, 0}, {2, 1, 0}},
      {{3, 2, 0}, {4, 3, 0}},
  };
  const auto decision = nsvu::DecideAdaptiveFrontierPrefix(
      candidates, {}, 100, 0, 0.20, 0.25);
  assert(decision.accepted_nodes == 1);
  assert(!decision.saw_batch_gain);
  assert(!decision.budget_limited);
}

void TestAdaptivePrefixEnforcesCumulativeCandidateBudget() {
  using Ref = nsvu::FrontierCandidateRef;
  const std::vector<std::vector<Ref>> candidates = {
      {{1, 0, 0}, {2, 1, 0}},
      {{3, 0, 0}, {4, 1, 0}},
  };
  const auto decision = nsvu::DecideAdaptiveFrontierPrefix(
      candidates, {}, 0, 0, 0.20, 0.25);
  assert(decision.accepted_nodes == 1);
  assert(decision.saw_batch_gain);
  assert(decision.budget_limited);
}

void TestAdaptivePrefixDeduplicatesVisitedCandidates() {
  using Ref = nsvu::FrontierCandidateRef;
  const std::vector<std::vector<Ref>> candidates = {
      {{1, 0, 0}, {2, 1, 0}},
      {{1, 0, 0}, {3, 1, 0}},
  };
  const auto decision = nsvu::DecideAdaptiveFrontierPrefix(
      candidates, {1}, 10, 0, 0.20, 0.25);
  assert(decision.first_candidates == 1);
  assert(decision.incremental_candidates == 1);
  assert(decision.accepted_nodes == 2);
}

}  // namespace

int main() {
  TestWidthOnePreservesSerialSelection();
  TestWindowStopsAtSearchLowerBound();
  TestWindowHonorsConfiguredWidth();
  TestZeroWidthIsRejected();
  TestAdaptivePrefixAcceptsObjectReuseWithinBudget();
  TestAdaptivePrefixRejectsNoObjectGain();
  TestAdaptivePrefixEnforcesCumulativeCandidateBudget();
  TestAdaptivePrefixDeduplicatesVisitedCandidates();
  return 0;
}
