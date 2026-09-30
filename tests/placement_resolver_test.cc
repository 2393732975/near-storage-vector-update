#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <vector>

#include "nsvu/placement.hpp"

int main() {
  const uint32_t shards = 5;
  const uint64_t points_per_object = 16;
  const uint64_t epoch = 7;
  ghnsw::PlacementResolver resolver(shards, points_per_object, epoch);

  const std::vector<uint64_t> ids = {
      0, 1, 4, 5, 79, 80, 81, 159, 160, 161, 100000000};
  for (const uint64_t id : ids) {
    const auto location = resolver.Resolve(id);
    if (location.group_id != 0 ||
        location.shard_id != static_cast<uint32_t>(id % shards) ||
        location.chunk_id != (id / shards) / points_per_object ||
        location.placement_epoch != epoch ||
        !(resolver.SelectForInsert(id) == location)) {
      std::cerr << "placement mismatch for id " << id << std::endl;
      return 1;
    }
    if (resolver.ResolveLabelShard(id) != id % shards) {
      std::cerr << "label placement mismatch for id " << id << std::endl;
      return 1;
    }
  }

  try {
    ghnsw::PlacementResolver invalid(0, points_per_object);
    (void)invalid;
    std::cerr << "zero shard count was accepted" << std::endl;
    return 1;
  } catch (const std::invalid_argument&) {
  }

  try {
    ghnsw::PlacementResolver invalid(shards, 0);
    (void)invalid;
    std::cerr << "zero points-per-object was accepted" << std::endl;
    return 1;
  } catch (const std::invalid_argument&) {
  }

  std::cout << "placement resolver tests passed" << std::endl;
  return 0;
}
