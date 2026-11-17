#include <atomic>
#include <cassert>
#include <map>
#include <string>
#include <thread>
#include <vector>
#include <iostream>

#include "nsvu/import_batches.hpp"
#include "nsvu/placement.hpp"

using Image = std::vector<std::map<std::string, std::string>>;

void Store(Image* image, std::vector<uint64_t>* offsets, uint64_t id) {
  ghnsw::PlacementResolver placement(5, 3);
  const auto where = placement.Resolve(id);
  const auto local = id / 5;
  if (local % 3 == 0) (*offsets)[where.shard_id] = 0;
  const auto prefix = std::to_string(where.chunk_id) + "/";
  const auto raw = std::string(id % 7 + 1, static_cast<char>(id));
  auto& object = (*image)[where.shard_id];
  object[prefix + "payload"] += raw;
  object[prefix + "ref/" + std::to_string(id)] = std::to_string((*offsets)[where.shard_id]);
  object[prefix + "adj/" + std::to_string(id)] = std::to_string(id + 1) + "," + std::to_string(id / 2);
  object["label/" + std::to_string(id)] = std::to_string(id);
  (*offsets)[where.shard_id] += raw.size();
}

int main() {
  // Compare complete synthetic payload/ref/adj/label images, including empty
  // owners, partial chunks and tail batches, against increasing global IDs.
  for (uint64_t count : {0, 1, 4, 5, 14, 15, 16, 79, 81, 103}) {
    Image serial(5);
    std::vector<uint64_t> serial_offsets(5);
    for (uint64_t id = 0; id < count; ++id) Store(&serial, &serial_offsets, id);
    for (uint32_t workers : {1, 2, 5}) {
      Image parallel(5);
      std::vector<uint64_t> parallel_offsets(5);
      std::vector<uint64_t> visits(count);
      const auto caller = std::this_thread::get_id();
      nsvu::RunImportOwners(5, workers,
          [&](uint32_t owner, const std::atomic<bool>&) {
        uint64_t local = 0;
        nsvu::ForEachImportId(count, 5, owner, [&](uint64_t id) {
          assert(id / 5 == local++);
          ++visits[id];
          Store(&parallel, &parallel_offsets, id);
        });
        assert(local == nsvu::ImportOwnerCount(count, 5, owner));
      }, [&] { assert(std::this_thread::get_id() == caller); });
      assert(parallel == serial);
      assert(parallel_offsets == serial_offsets);
      for (auto n : visits) assert(n == 1);
    }
  }
  assert(nsvu::ImportOwnerCount(UINT64_MAX, 5, 4) == (UINT64_MAX - 5) / 5 + 1);
  assert(!nsvu::OmapMustFlush(0, 0, 80, 2, 100));
  assert(nsvu::OmapMustFlush(1, 80, 21, 2, 100));
  assert(!nsvu::OmapMustFlush(1, 80, 20, 2, 100));
  assert(nsvu::OmapMustFlush(2, 80, 1, 2, 100));
  assert(nsvu::OmapEntryBytes(3, 7) == 26);
  try { nsvu::OmapMustFlush(0, 0, 101, 2, 100); assert(false); }
  catch (const std::invalid_argument&) {}
  try { nsvu::OmapEntryBytes(0, UINT64_MAX); assert(false); }
  catch (const std::overflow_error&) {}
  try { nsvu::RunImportOwners(5, 6, [](uint32_t, const auto&) {}, [] {}); assert(false); }
  catch (const std::invalid_argument&) {}
  std::atomic<int> active{0};
  bool propagated = false;
  try {
    nsvu::RunImportOwners(5, 5, [&](uint32_t owner, const std::atomic<bool>&) {
      struct Guard { std::atomic<int>& n; Guard(std::atomic<int>& value) : n(value) { ++n; } ~Guard() { --n; } } guard(active);
      if (owner == 1) throw std::runtime_error("injected write failure");
      std::this_thread::yield();
    }, [] {});
  } catch (const std::runtime_error& e) {
    propagated = std::string(e.what()) == "injected write failure";
  }
  assert(propagated && active == 0);
  std::cout << "import partition, content, batch bound and failure tests passed\n";
}
