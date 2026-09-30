#pragma once

#include <cstdint>
#include <stdexcept>

namespace ghnsw {

struct PhysicalLocation {
  uint32_t group_id = 0;
  uint32_t shard_id = 0;
  uint64_t chunk_id = 0;
  uint64_t placement_epoch = 0;

  bool operator==(const PhysicalLocation& other) const {
    return group_id == other.group_id && shard_id == other.shard_id &&
        chunk_id == other.chunk_id && placement_epoch == other.placement_epoch;
  }
};

// The first resolver implementation deliberately preserves the original
// modulo layout. Later placement policies can change this implementation
// without duplicating routing arithmetic across importer, coordinator, and
// checker code.
class PlacementResolver {
 public:
  PlacementResolver(
      uint32_t shard_count,
      uint64_t points_per_object,
      uint64_t placement_epoch = 0)
      : shard_count_(shard_count),
        points_per_object_(points_per_object),
        placement_epoch_(placement_epoch) {
    if (shard_count_ == 0 || points_per_object_ == 0) {
      throw std::invalid_argument(
          "shard_count and points_per_object must be positive");
    }
  }

  PhysicalLocation Resolve(uint64_t global_id) const {
    const uint32_t shard_id = static_cast<uint32_t>(global_id % shard_count_);
    const uint64_t local_id = global_id / shard_count_;
    return {0, shard_id, local_id / points_per_object_, placement_epoch_};
  }

  PhysicalLocation SelectForInsert(uint64_t global_id) const {
    return Resolve(global_id);
  }

  uint32_t ResolveLabelShard(uint64_t external_label) const {
    return static_cast<uint32_t>(external_label % shard_count_);
  }

  uint32_t shard_count() const { return shard_count_; }
  uint64_t points_per_object() const { return points_per_object_; }
  uint64_t placement_epoch() const { return placement_epoch_; }

 private:
  uint32_t shard_count_;
  uint64_t points_per_object_;
  uint64_t placement_epoch_;
};

}  // namespace ghnsw
