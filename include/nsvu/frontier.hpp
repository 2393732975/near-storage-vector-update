#pragma once

#include <cstddef>
#include <stdexcept>
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

}  // namespace nsvu
