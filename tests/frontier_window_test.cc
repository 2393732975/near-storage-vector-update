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

}  // namespace

int main() {
  TestWidthOnePreservesSerialSelection();
  TestWindowStopsAtSearchLowerBound();
  TestWindowHonorsConfiguredWidth();
  TestZeroWidthIsRejected();
  return 0;
}
