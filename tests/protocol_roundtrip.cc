#include "include/ceph_assert.h"

#include <cstdint>
#include <iostream>
#include <stdexcept>

#include "nsvu/protocol.hpp"

namespace {

void Require(bool condition, const char* message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void TestEdgePatchBatch() {
  ghnsw::EdgePatchBatchRequest request;
  request.update_id = 101;
  request.max_neighbors = 24;
  ghnsw::AdjacencyBlob patch;
  patch.global_id = 42;
  patch.level_count = 2;
  patch.neighbors = {{7, 9}, {11}};
  request.entries.push_back(patch);

  ceph::bufferlist encoded;
  request.encode(encoded);
  auto iterator = encoded.cbegin();
  ghnsw::EdgePatchBatchRequest decoded;
  decoded.decode(iterator);

  Require(decoded.update_id == 101, "patch update_id did not round-trip");
  Require(decoded.max_neighbors == 24, "max_neighbors did not round-trip");
  Require(decoded.entries.size() == 1, "patch count did not round-trip");
  Require(decoded.entries[0].global_id == 42, "patch id did not round-trip");
  Require(decoded.entries[0].level_count == 2, "level count did not round-trip");
  Require(decoded.entries[0].neighbors == patch.neighbors, "neighbors did not round-trip");
}

void TestInsertLifecycle() {
  ghnsw::ReserveInsertRequest reserve;
  reserve.update_id = 202;
  ceph::bufferlist reserve_encoded;
  reserve.encode(reserve_encoded);
  auto reserve_it = reserve_encoded.cbegin();
  ghnsw::ReserveInsertRequest reserve_decoded;
  reserve_decoded.decode(reserve_it);
  Require(reserve_decoded.update_id == reserve.update_id,
          "reservation update_id did not round-trip");

  ghnsw::ReserveInsertReply reply;
  reply.global_id = 303;
  reply.search_meta.next_global_id = 303;
  reply.search_meta.cur_element_count = 300;
  ceph::bufferlist reply_encoded;
  reply.encode(reply_encoded);
  auto reply_it = reply_encoded.cbegin();
  ghnsw::ReserveInsertReply reply_decoded;
  reply_decoded.decode(reply_it);
  Require(reply_decoded.global_id == reply.global_id,
          "reserved global_id did not round-trip");
  Require(reply_decoded.search_meta.next_global_id == 303 &&
              reply_decoded.search_meta.cur_element_count == 300,
          "reservation search meta did not round-trip");

  ghnsw::FinalizeInsertRequest finalize;
  finalize.update_id = 202;
  finalize.global_id = 303;
  finalize.level = 4;
  ceph::bufferlist finalize_encoded;
  finalize.encode(finalize_encoded);
  auto finalize_it = finalize_encoded.cbegin();
  ghnsw::FinalizeInsertRequest finalize_decoded;
  finalize_decoded.decode(finalize_it);
  Require(finalize_decoded.update_id == finalize.update_id &&
              finalize_decoded.global_id == finalize.global_id &&
              finalize_decoded.level == finalize.level,
          "finalize request did not round-trip");
}

void TestVectorRef() {
  ghnsw::VectorRef reference;
  reference.global_id = 99;
  reference.external_label = 1234;
  reference.offset = 4096;
  reference.bytes = 512;
  reference.dim = 128;
  reference.vector_kind = ghnsw::kVectorKindF32;
  reference.flags = ghnsw::kVectorFlagActive;
  reference.level = 3;

  ceph::bufferlist encoded;
  reference.encode(encoded);
  auto iterator = encoded.cbegin();
  ghnsw::VectorRef decoded;
  decoded.decode(iterator);

  Require(decoded.global_id == reference.global_id, "global_id did not round-trip");
  Require(decoded.external_label == reference.external_label,
          "external_label did not round-trip");
  Require(decoded.offset == reference.offset && decoded.bytes == reference.bytes,
          "payload location did not round-trip");
  Require(decoded.dim == reference.dim && decoded.vector_kind == reference.vector_kind,
          "vector schema did not round-trip");
  Require(decoded.flags == reference.flags && decoded.level == reference.level,
          "vector state did not round-trip");
}

void TestCasLabel() {
  ghnsw::CasLabelRequest request;
  request.external_label = 17;
  request.expected_global_id = 23;
  request.replacement_global_id = 29;
  request.expect_missing = false;

  ceph::bufferlist encoded;
  request.encode(encoded);
  auto iterator = encoded.cbegin();
  ghnsw::CasLabelRequest decoded;
  decoded.decode(iterator);

  Require(decoded.external_label == request.external_label,
          "label did not round-trip");
  Require(decoded.expected_global_id == request.expected_global_id,
          "expected label target did not round-trip");
  Require(decoded.replacement_global_id == request.replacement_global_id,
          "replacement label target did not round-trip");
  Require(decoded.expect_missing == request.expect_missing,
          "expect_missing did not round-trip");
}

}  // namespace

int main() {
  try {
    TestEdgePatchBatch();
    TestInsertLifecycle();
    TestVectorRef();
    TestCasLabel();
    std::cout << "protocol round-trip tests passed\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "protocol round-trip test failed: " << error.what() << "\n";
    return 1;
  }
}
