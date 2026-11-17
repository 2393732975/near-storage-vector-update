#include <cassert>
#include <limits>

#include "nsvu/baseline_observation.hpp"

int main() {
  using ghnsw::ActiveQueryCandidate;
  const auto selected = ghnsw::SelectActiveQueryResults({
      {1, 101, 1, 0.1f, false},  // stale
      {2, 102, 22, 0.2f, true},  // label points to a newer version
      {3, 103, 3, 0.3f, true},
      {3, 103, 3, 0.3f, true},  // duplicate label
      {4, 104, 4, 0.4f, true},
      {5, 105, 5, std::numeric_limits<float>::infinity(), true},
      {6, 106, 6, std::numeric_limits<float>::quiet_NaN(), true}}, 10);
  assert(selected.size() == 2);
  assert(selected[0].external_label == 103);
  assert(selected[1].external_label == 104);
  assert(ghnsw::SelectActiveQueryResults({{1, 1, 1, 1.0f, true}}, 0).empty());
  const auto limited = ghnsw::SelectActiveQueryResults({
      {2, 12, 2, 1.0f, true}, {1, 11, 1, 1.0f, true}}, 1);
  assert(limited.size() == 1 && limited[0].external_label == 11);
  assert(ghnsw::RpcLatencyBucketUs(0.0) == 1);
  assert(ghnsw::RpcLatencyBucketUs(0.000003) == 4);
  assert(ghnsw::RpcLatencyBucketUs(0.001) == 1024);
}
