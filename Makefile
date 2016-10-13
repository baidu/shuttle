# Modify prefix to specify the directory to install shuttle
PREFIX = ./output

# Include dependency path information in depends.mk
include depends.mk

# Compiler related
OPT ?= -O2 -g2
CXX = g++
INCPATH = -I./ -I./src -I$(SOFA_PBRPC_DIR)/src -I$(PROTOBUF_DIR)/include -I$(BOOST_HEADER_DIR) \
		  -I$(GFLAGS_DIR)/include -I$(SNAPPY_DIR)/include -I$(LIB_HDFS_DIR)/output/include \
		  -I$(GALAXY_DIR)/output/include -I$(GALAXY_DIR)/common/include \
		  -I$(GALAXY_DIR)/ins/output/include
CXXFLAGS += $(OPT) -pipe -W -Wall -fPIC \
			-D_GNU_SOURCE -D__STDC_LIMIT_MACROS -DHAVE_SNAPPY $(INCPATH)
LDFLAGS += -lpthread -lz -lrt -lprotobuf -lsnappy \
		   -L$(SOFA_PBRPC_DIR) -L$(PROTOBUF_DIR)/lib -L$(GFLAGS_DIR)/lib \
		   -L$(SNAPPY_DIR)/lib -L$(LIB_HDFS_DIR)/output/lib -L$(GALAXY_DIR) \
		   -L$(GALAXY_DIR)/common -L$(GALAXY_DIR)/ins/output/lib
PROTOC = $(PROTOBUF_DIR)/bin/protoc

# Source related constants
PROTO_FILE = $(wildcard proto/*.proto)
PROTO_SRC = $(patsubst %.proto, %.pb.cc, $(PROTO_FILE))
PROTO_HEADER = $(patsubst %.proto, %.pb.h, $(PROTO_FILE))
PROTO_OBJ = $(patsubst %.cc, %.o, $(PROTO_SRC))

FORMAT_FILE_TYPES_SRC = $(wildcard src/common/format/*.cc) \
						proto/sortfile.pb.cc src/common/fileformat.cc
FORMAT_FILE_TYPES_HEADER = $(patsubst %.cc, %.h, $(FORMAT_FILE_TYPES_SRC))
FILE_TYPES_SRC = src/common/file.cc $(wildcard src/common/file/*.cc)
FILE_TYPES_HEADER = $(patsubst %.cc, %.h, $(FILE_TYPES_SRC))
SCANNER_SUPPORT_SRC = src/common/scanner.cc $(FORMAT_FILE_TYPES_SRC) $(FILE_TYPES_SRC)
SCANNER_SUPPORT_HEADER = $(patsubst %.cc, %.h, $(SCANNER_SUPPORT_SRC))

MASTER_SRC = $(wildcard src/master/*.cc) \
			 $(PROTO_SRC) $(SCANNER_SUPPORT_SRC) \
			 src/common/dag_scheduler.cc
MASTER_HEADER = $(wildcard src/master/*.h) \
				$(PROTO_HEADER) $(SCANNER_SUPPORT_SRC) \
				src/common/dag_scheduler.h src/common/rpc_client.h
MASTER_OBJ = $(patsubst %.cc, %.o, $(MASTER_SRC))

MINION_SRC = $(wildcard src/minion/container/*.cc) \
			 proto/shuttle.pb.cc proto/minion.pb.cc proto/master.pb.cc \
			 $(FILE_TYPES_SRC) $(FORMAT_FILE_TYPES_SRC) \
			 src/common/dag_scheduler.cc
MINION_HEADER = $(wildcard src/minion/container/*.h) \
				proto/shuttle.pb.h proto/minion.pb.h proto/master.pb.h \
				$(FILE_TYPES_HEADER) $(FORMAT_FILE_TYPES_HEADER) \
				src/common/dag_scheduler.h src/common/rpc_client.h
MINION_OBJ = $(patsubst %.cc, %.o, $(MINION_SRC))

INLET_SRC = $(wildcard src/minion/input/*.cc) \
			proto/shuttle.pb.cc $(SCANNER_SUPPORT_SRC)
INLET_HEADER = $(wildcard src/minion/input/*.h) \
			   proto/shuttle.pb.h $(SCANNER_SUPPORT_HEADER) \
			   src/minion/common/log_name.h
INLET_OBJ = $(patsubst %.cc, %.o, $(INLET_SRC))

COMBINER_SRC = $(wildcard src/minion/combiner/*.cc) \
			   proto/shuttle.pb.cc \
			   src/minion/common/streaming.cc src/minion/common/emitter.cc \
			   src/minion/output/partition.cc
COMBINER_HEADER = $(wildcard src/minion/combiner/*.h) \
				  proto/shuttle.pb.h \
				  $(FILE_TYPES_SRC) $(FORMAT_FILE_TYPES_SRC) \
				  src/minion/common/streaming.h src/minion/common/emitter.h \
				  src/minion/common/log_name.h src/minion/output/partition.h \
				  src/common/format/plain_text.h
COMBINER_OBJ = $(patsubst %.cc, %.o, $(COMBINER_SRC))

OUTLET_SRC = $(wildcard src/minion/output/*.cc) \
			 proto/shuttle.pb.cc \
			 $(FILE_TYPES_SRC) $(FORMAT_FILE_TYPES_SRC) \
			 src/minion/common/streaming.cc src/minion/common/emitter.cc
OUTLET_HEADER = $(wildcard src/minion/output/*.h) \
				proto/shuttle.pb.h src/minion/common/log_name.h \
				$(FILE_TYPES_HEADER) $(FORMAT_FILE_TYPES_HEADER) \
				src/minion/common/streaming.h src/minion/common/emitter.h
OUTLET_OBJ = $(patsubst %.cc, %.o, $(OUTLET_SRC))

TEST_FILE_SRC = src/test/file.test.cc proto/shuttle.pb.cc \
				$(FILE_TYPES_SRC)
TEST_FILE_OBJ = $(patsubst %.cc, %.o, $(TEST_FILE_SRC))

TEST_FILE_FORMAT_SRC = src/test/fileformat.test.cc \
					   $(FORMAT_FILE_TYPES_SRC)
TEST_FILE_FORMAT_OBJ = $(patsubst %.cc, %.o, $(TEST_FILE_FORMAT_SRC))

TEST_SCANNER_SRC = src/test/scanner.test.cc \
				   $(SCANNER_SUPPORT_SRC)
TEST_SCANNER_OBJ = $(patsubst %.cc, %.o, $(TEST_SCANNER_SRC))

TEST_MERGER_SRC = src/test/merger.test.cc src/minion/input/merger.cc \
				  proto/shuttle.pb.cc $(SCANNER_SUPPORT_SRC)
TEST_MERGER_OBJ = $(patsubst %.cc, %.o, $(TEST_MERGER_SRC))

TEST_DAG_SCHEDULER_SRC = src/test/dag_scheduler.test.cc \
						 src/common/dag_scheduler.cc \
						 proto/shuttle.proto
TEST_DAG_SCHEDULER_OBJ = $(patsubst %.cc, %.o, $(TEST_DAG_SCHEDULER_SRC))

TEST_RESOURCE_MANAGER_SRC = src/test/resource_manager.test.cc \
							src/master/resource_manager.cc src/master/master_flags.cc \
							proto/shuttle.proto $(SCANNER_SUPPORT_SRC)
TEST_RESOURCE_MANAGER_OBJ = $(patsubst %.cc, %.o, $(TEST_RESOURCE_MANAGER_SRC))

TOOL_PARTITION_SRC = src/tool/partition_tool.cc src/minion/output/partition.cc \
					 proto/shuttle.pb.cc
TOOL_PARTITION_OBJ = $(patsubst %.cc, %.o, $(TOOL_PARTITION_OBJ))

TOOL_SORTFILE_SRC = src/tool/sortfile_tool.cc src/minion/input/merger.cc \
					proto/shuttle.pb.cc $(SCANNER_SUPPORT_SRC)
TOOL_SORTFILE_OBJ = $(patsubst %.cc, %.o, $(TOOL_SORTFILE_SRC))

OBJS = $(MASTER_OBJ) $(MINION_OBJ) $(INLET_OBJ) $(COMBINER_OBJ) $(OUTLET_OBJ)
BIN = master minion inlet combiner outlet
TESTS = file_test fileformat_test scanner_test merger_test dag_test rm_test

# Default build all binary files except tests
all: $(BIN)

# Dependencies
$(MASTER_OBJ): $(MASTER_HEADER)
$(MINION_OBJ): $(MINION_HEADER)
$(INLET_OBJ): $(INLET_HEADER)
$(COMBINER_OBJ): $(COMBINER_HEADER)
$(OUTLET_OBJ): $(OUTLET_HEADER)

# Building targets
%.pb.cc %.pb.h: %.proto
	$(PROTOC) --proto_path=./proto --proto_path=$(PROTOBUF_DIR)/include --cpp_out=./proto $<

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(INCPATH) -c $< -o $@

master: $(MASTER_OBJ)
	$(CXX) $(MASTER_OBJ) -o $@ $(LDFLAGS)

minion: $(MINION_OBJ)
	$(CXX) $(MINION_OBJ) -o $@ $(LDFLAGS)

inlet: $(INLET_OBJ)
	$(CXX) $(INLET_OBJ) -o $@ $(LDFLAGS)

combiner: $(COMBINER_OBJ)
	$(CXX) $(COMBINER_OBJ) -o $@ $(LDFLAGS)

outlet: $(OUTLET_OBJ)
	$(CXX) $(OUTLET_OBJ) -o $@ $(LDFLAGS)

file_test: $(TEST_FILE_OBJ)
	$(CXX) $(TEST_FILE_OBJ) -o $@ $(LDFLAGS)

fileformat_test: $(TEST_FILE_FORMAT_OBJ)
	$(CXX) $(TEST_FILE_FORMAT_OBJ) -o $@ $(LDFLAGS)

scanner_test: $(TEST_SCANNER_OBJ)
	$(CXX) $(TEST_SCANNER_OBJ) -o $@ $(LDFLAGS)

merger_test: $(TEST_MERGER_OBJ)
	$(CXX) $(TEST_MERGER_OBJ) -o $@ $(LDFLAGS)

dag_test: $(TEST_DAG_SCHEDULER_OBJ)
	$(CXX) $(TEST_DAG_SCHEDULER_OBJ) -o $@ $(LDFLAGS)

rm_test: $(TEST_RESOURCE_MANAGER_OBJ)
	$(CXX) $(TEST_RESOURCE_MANAGER_OBJ) -o $@ $(LDFLAGS)

.PHONY: clean install output
clean:
	rm -rf output/
	rm -rf $(BIN) $(TESTS) $(OBJS)
	rm -rf $(PROTO_SRC) $(PROTO_HEADER)

output: $(BIN)
	mkdir -p output/bin
	cp $(BIN) output/bin

