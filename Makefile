CEPH_SRC ?=
HNSWLIB_INCLUDE ?=
BUILD_DIR := build
CXX ?= g++
CPPFLAGS := -I$(CEPH_SRC) -I$(CEPH_SRC)/include -I$(CEPH_SRC)/objclass -Iinclude
CXXFLAGS := -O2 -std=gnu++17 -Wall -Wextra
LDFLAGS := -L/usr/lib/ceph -Wl,-rpath,/usr/lib/ceph
LDLIBS := -lrados /usr/lib/ceph/libceph-common.so.2 -pthread

.PHONY: all check clean check-config

all: check-config $(BUILD_DIR)/libcls_hnsw_global.so $(BUILD_DIR)/nsvu-update-coordinator $(BUILD_DIR)/nsvu-base-importer $(BUILD_DIR)/nsvu-index-checker

check-config:
	@test -n "$(CEPH_SRC)" || (echo "Set CEPH_SRC to the Ceph src directory."; exit 2)
	@test -n "$(HNSWLIB_INCLUDE)" || (echo "Set HNSWLIB_INCLUDE to hnswlib/include."; exit 2)

$(BUILD_DIR):
	mkdir -p $@

$(BUILD_DIR)/libcls_hnsw_global.so: src/cls/hnsw_cls.cc include/nsvu/protocol.hpp | $(BUILD_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -fPIC -shared -o $@ $<

$(BUILD_DIR)/nsvu-update-coordinator: src/coordinator/update_coordinator.cc include/nsvu/protocol.hpp | $(BUILD_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS)

$(BUILD_DIR)/nsvu-base-importer: src/importer/base_index_importer.cc include/nsvu/protocol.hpp | $(BUILD_DIR)
	$(CXX) $(CPPFLAGS) -I$(HNSWLIB_INCLUDE) $(CXXFLAGS) -fopenmp $(LDFLAGS) -o $@ $< $(LDLIBS)

$(BUILD_DIR)/nsvu-index-checker: src/tools/index_checker.cc include/nsvu/protocol.hpp | $(BUILD_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(LDFLAGS) -o $@ $< $(LDLIBS)

$(BUILD_DIR)/protocol-roundtrip-test: tests/protocol_roundtrip.cc include/nsvu/protocol.hpp | $(BUILD_DIR)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(LDFLAGS) -o $@ $< /usr/lib/ceph/libceph-common.so.2 -pthread

check: check-config $(BUILD_DIR)/protocol-roundtrip-test
	$(BUILD_DIR)/protocol-roundtrip-test

clean:
	rm -rf $(BUILD_DIR)
