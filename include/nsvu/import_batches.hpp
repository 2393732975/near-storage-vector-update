#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace nsvu {

inline uint64_t ImportOwnerCount(uint64_t total, uint32_t owners, uint32_t owner) {
  if (owners == 0 || owner >= owners) {
    throw std::invalid_argument("invalid import owner partition");
  }
  return total > owner ? (total - 1 - owner) / owners + 1 : 0;
}

// Only the current modulo placement is supported by the importer. The owner
// partition retains exactly the original increasing-ID order within an object.
template <typename Function>
void ForEachImportId(uint64_t total, uint32_t owners, uint32_t owner, Function function) {
  const uint64_t count = ImportOwnerCount(total, owners, owner);
  for (uint64_t local = 0; local < count; ++local) {
    function(local * owners + owner);
  }
}

inline uint64_t OmapEntryBytes(uint64_t key_bytes, uint64_t value_bytes) {
  if (value_bytes > UINT64_MAX - 16 || key_bytes > UINT64_MAX - value_bytes - 16) {
    throw std::overflow_error("OMAP entry length overflow");
  }
  // Key/value lengths and map-entry overhead; transport envelope is not counted.
  return key_bytes + value_bytes + 16;
}

inline bool OmapMustFlush(
    uint64_t entries, uint64_t bytes, uint64_t next_bytes,
    uint64_t entry_limit, uint64_t byte_limit) {
  if (entry_limit == 0 || byte_limit == 0 || next_bytes > byte_limit || bytes > byte_limit) {
    throw std::invalid_argument("OMAP entry exceeds configured batch bounds");
  }
  return entries > 0 &&
      (entries >= entry_limit || bytes > byte_limit - next_bytes);
}

// All workers join before propagating an error. Progress always runs on the
// calling thread; it can safely replace one progress JSON without races.
template <typename Job, typename Progress>
void RunImportOwners(uint32_t owners, uint32_t workers, Job job, Progress progress) {
  if (owners == 0 || workers == 0 || workers > owners) {
    throw std::invalid_argument("import workers must be in [1, owners]");
  }
  std::atomic<uint32_t> next{0}, running{0};
  std::atomic<bool> stopped{false};
  std::exception_ptr error;
  std::mutex mutex;
  std::condition_variable changed;
  std::vector<std::thread> threads;
  auto run = [&] {
    try {
      while (!stopped.load()) {
        const auto owner = next.fetch_add(1);
        if (owner >= owners) break;
        job(owner, stopped);
      }
    } catch (...) {
      std::lock_guard<std::mutex> lock(mutex);
      if (!error) error = std::current_exception();
      stopped.store(true);
    }
    running.fetch_sub(1);
    changed.notify_one();
  };
  try {
    for (uint32_t i = 0; i < std::min(owners, workers); ++i) {
      running.fetch_add(1);
      try {
        threads.emplace_back(run);
      } catch (...) {
        running.fetch_sub(1);
        throw;
      }
    }
  } catch (...) {
    stopped.store(true);
    for (auto& thread : threads) thread.join();
    throw;
  }
  try {
    while (running.load() != 0) {
      progress();
      std::unique_lock<std::mutex> lock(mutex);
      changed.wait_for(lock, std::chrono::seconds(1), [&] { return running.load() == 0; });
    }
    progress();
  } catch (...) {
    stopped.store(true);
    for (auto& thread : threads) thread.join();
    throw;
  }
  for (auto& thread : threads) thread.join();
  if (error) std::rethrow_exception(error);
}

}  // namespace nsvu
