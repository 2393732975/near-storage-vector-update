#include <rados/librados.hpp>

#include "include/ceph_assert.h"

#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <stdexcept>
#include <string>

#include "nsvu/protocol.hpp"

namespace {

struct Config {
  std::string keyring;
  std::string pool = "nsvu_meta";
  std::string oid = "hnsw.global.meta";
  std::string output;
  bool confirm_mutation = false;
};

[[noreturn]] void Fail(const std::string& message) {
  throw std::runtime_error(message);
}

void CheckResult(int result, const std::string& operation) {
  if (result < 0) {
    Fail(operation + " failed with " + std::to_string(result));
  }
}

template <typename Request, typename Reply>
Reply Exec(
    librados::IoCtx& ioctx,
    const std::string& oid,
    const char* method,
    ghnsw::Opcode opcode,
    const ghnsw::RequestEnvelope& envelope,
    const Request& request) {
  ceph::bufferlist input = ghnsw::EncodeRequest(envelope, request);
  ceph::bufferlist output;
  CheckResult(ioctx.exec(oid, "hnsw_global", method, input, output), method);
  ghnsw::RequestEnvelope response_envelope;
  Reply reply;
  CheckResult(
      ghnsw::DecodeResponse(output, opcode, &response_envelope, &reply),
      std::string("decode ") + method);
  if (response_envelope.request_id != envelope.request_id ||
      response_envelope.update_id != envelope.update_id ||
      response_envelope.placement_epoch != envelope.placement_epoch) {
    Fail(std::string(method) + " returned a mismatched response envelope");
  }
  return reply;
}

ghnsw::GlobalMeta GetMeta(
    librados::IoCtx& ioctx, const Config& config, uint64_t request_id) {
  ghnsw::RequestEnvelope envelope;
  envelope.opcode = static_cast<uint32_t>(ghnsw::Opcode::kGetGlobalMeta);
  envelope.request_id = request_id;
  const auto reply = Exec<ghnsw::EmptyRequest, ghnsw::GetGlobalMetaReply>(
      ioctx,
      config.oid,
      "get_global_meta",
      ghnsw::Opcode::kGetGlobalMeta,
      envelope,
      {});
  CheckResult(reply.status, "get_global_meta reply");
  return reply.meta;
}

void ExecMutation(
    librados::IoCtx& ioctx,
    const std::string& oid,
    const char* method,
    ceph::bufferlist& input) {
  ceph::bufferlist ignored_output;
  CheckResult(
      ioctx.exec(oid, "hnsw_global", method, input, ignored_output), method);
}

ghnsw::ReserveInsertReply ReadReservation(
    librados::IoCtx& ioctx, const std::string& oid, uint64_t update_id) {
  const std::string key = ghnsw::ReservationKey(update_id);
  std::set<std::string> keys{key};
  std::map<std::string, ceph::bufferlist> values;
  CheckResult(ioctx.omap_get_vals_by_keys(oid, keys, &values),
              "read reservation marker");
  auto found = values.find(key);
  if (found == values.end()) {
    Fail("reservation marker is missing");
  }
  ghnsw::ReserveInsertReply reply;
  auto iterator = found->second.cbegin();
  try {
    reply.decode(iterator);
  } catch (const ceph::buffer::error&) {
    Fail("reservation marker is malformed");
  }
  CheckResult(reply.status, "stored reservation reply");
  return reply;
}

ghnsw::RequestEnvelope MutationEnvelope(
    ghnsw::Opcode opcode, uint64_t request_id, uint64_t update_id) {
  ghnsw::RequestEnvelope envelope;
  envelope.opcode = static_cast<uint32_t>(opcode);
  envelope.request_id = request_id;
  envelope.update_id = update_id;
  envelope.placement_epoch = 1;
  return envelope;
}

Config ParseArgs(int argc, char** argv) {
  Config config;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto next = [&](const char* name) -> std::string {
      if (++i >= argc) {
        Fail(std::string("missing value for ") + name);
      }
      return argv[i];
    };
    if (arg == "--keyring") {
      config.keyring = next("--keyring");
    } else if (arg == "--pool") {
      config.pool = next("--pool");
    } else if (arg == "--oid") {
      config.oid = next("--oid");
    } else if (arg == "--output") {
      config.output = next("--output");
    } else if (arg == "--confirm-mutation") {
      config.confirm_mutation = true;
    } else {
      Fail("unknown argument: " + arg);
    }
  }
  if (config.keyring.empty()) {
    Fail("--keyring is required");
  }
  if (!config.confirm_mutation) {
    Fail("refusing to mutate CLS metadata without --confirm-mutation");
  }
  return config;
}

void RequireEqual(uint64_t actual, uint64_t expected, const char* field) {
  if (actual != expected) {
    Fail(std::string(field) + " mismatch: expected " +
         std::to_string(expected) + ", got " + std::to_string(actual));
  }
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const Config config = ParseArgs(argc, argv);
    librados::Rados cluster;
    CheckResult(cluster.init2("client.admin", "client", 0), "cluster init");
    CheckResult(cluster.conf_read_file("/etc/ceph/ceph.conf"), "read config");
    CheckResult(cluster.conf_set("keyring", config.keyring.c_str()), "set keyring");
    CheckResult(cluster.connect(), "cluster connect");
    librados::IoCtx ioctx;
    CheckResult(cluster.ioctx_create(config.pool.c_str(), ioctx), "open pool");

    const uint64_t update_id = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    const ghnsw::GlobalMeta before = GetMeta(ioctx, config, 1);

    ghnsw::ReserveInsertRequest reserve_request;
    reserve_request.update_id = update_id;
    const auto reserve_envelope = MutationEnvelope(
        ghnsw::Opcode::kReserveInsertId, 2, update_id);
    ceph::bufferlist reserve_input =
        ghnsw::EncodeRequest(reserve_envelope, reserve_request);
    ExecMutation(ioctx, config.oid, "reserve_insert_id", reserve_input);
    const auto first_reserve = ReadReservation(ioctx, config.oid, update_id);
    const ghnsw::GlobalMeta after_first_reserve = GetMeta(ioctx, config, 3);

    // Simulate a lost response by replaying the exact same request envelope and
    // payload. The reservation and global metadata must not advance again.
    ExecMutation(ioctx, config.oid, "reserve_insert_id", reserve_input);
    const auto replayed_reserve = ReadReservation(ioctx, config.oid, update_id);
    const ghnsw::GlobalMeta after_replayed_reserve = GetMeta(ioctx, config, 4);
    RequireEqual(replayed_reserve.global_id, first_reserve.global_id, "reserved global_id");
    RequireEqual(after_first_reserve.next_global_id, before.next_global_id + 1,
                 "next_global_id after reserve");
    RequireEqual(after_first_reserve.version, before.version + 1,
                 "version after reserve");
    RequireEqual(after_replayed_reserve.next_global_id,
                 after_first_reserve.next_global_id,
                 "next_global_id after reserve replay");
    RequireEqual(after_replayed_reserve.version, after_first_reserve.version,
                 "version after reserve replay");

    ghnsw::FinalizeInsertRequest finalize_request;
    finalize_request.update_id = update_id;
    finalize_request.global_id = first_reserve.global_id;
    finalize_request.level = 0;
    const auto finalize_envelope = MutationEnvelope(
        ghnsw::Opcode::kFinalizeInsert, 5, update_id);
    ceph::bufferlist finalize_input =
        ghnsw::EncodeRequest(finalize_envelope, finalize_request);
    ExecMutation(ioctx, config.oid, "finalize_insert", finalize_input);
    const ghnsw::GlobalMeta after_first_finalize = GetMeta(ioctx, config, 6);

    ExecMutation(ioctx, config.oid, "finalize_insert", finalize_input);
    const ghnsw::GlobalMeta after_replayed_finalize = GetMeta(ioctx, config, 7);
    RequireEqual(after_first_finalize.cur_element_count,
                 after_replayed_reserve.cur_element_count + 1,
                 "cur_element_count after finalize");
    RequireEqual(after_first_finalize.version, after_replayed_reserve.version + 1,
                 "version after finalize");
    RequireEqual(after_replayed_finalize.cur_element_count,
                 after_first_finalize.cur_element_count,
                 "cur_element_count after finalize replay");
    RequireEqual(after_replayed_finalize.version, after_first_finalize.version,
                 "version after finalize replay");

    std::string json =
        "{\n"
        "  \"status\": \"pass\",\n"
        "  \"protocol_schema_version\": " +
        std::to_string(ghnsw::kProtocolSchemaVersion) +
        ",\n  \"failure_model\": \"discard response and replay identical request\""
        ",\n  \"update_id\": " + std::to_string(update_id) +
        ",\n  \"reserved_global_id\": " +
        std::to_string(first_reserve.global_id) +
        ",\n  \"next_global_id_before\": " +
        std::to_string(before.next_global_id) +
        ",\n  \"next_global_id_after_replay\": " +
        std::to_string(after_replayed_reserve.next_global_id) +
        ",\n  \"cur_element_count_before\": " +
        std::to_string(before.cur_element_count) +
        ",\n  \"cur_element_count_after_replay\": " +
        std::to_string(after_replayed_finalize.cur_element_count) +
        ",\n  \"reserve_replay_advanced_meta\": false,\n"
        "  \"finalize_replay_advanced_meta\": false\n}\n";
    if (config.output.empty()) {
      std::cout << json;
    } else {
      std::ofstream output(config.output);
      if (!output) {
        Fail("cannot open output: " + config.output);
      }
      output << json;
    }

    ioctx.close();
    cluster.shutdown();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "error: " << error.what() << std::endl;
    return 1;
  }
}
