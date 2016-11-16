# Modify prefix to specify the directory to install shuttle
PREFIX = ./output

# Include dependency path information in depends.mk
include depends.mk

# Compiler related
OPT ?= -O2 -g2
CXX = g++
INCPATH = -I./src -I$(BOOST_DIR) -I$(LIB_HDFS_DIR)/output/include \
		  -I$(GALAXY_DIR)/src/sdk -I$(GALAXY_DIR)/thirdparty/include \
		  -I$(GALAXY_DIR)/thirdparty/rapidjson/include
CXXFLAGS += $(OPT) -pipe -MMD -W -Wall -fPIC \
			-D_GNU_SOURCE -D__STDC_LIMIT_MACROS -DHAVE_SNAPPY $(INCPATH)
BASIC_LD_FLAGS = -L$(GALAXY_DIR)/thirdparty/lib \
				 -lsofa-pbrpc -lins_sdk -lprotobuf -lsnappy -lgflags -lcommon \
				 -lpthread -lrt -lz
LDFLAGS += -L$(GALAXY_DIR) -lgalaxy_sdk \
		   $(BASIC_LD_FLAGS) \
		   -L$(GALAXY_DIR)/thirdparty/lib -lglog -lsnappy \
		   -L$(LIB_HDFS_DIR)/output/lib -lhdfs \
		   -L$(JVM_LIB_DIR) -ljvm -lz
TESTFLAGS = -L$(GALAXY_DIR)/thirdparty/lib -lgtest
PROTOC = $(GALAXY_DIR)/thirdparty/bin/protoc

# Source related constants
PROTO_FILE = $(wildcard src/proto/*.proto)
PROTO_SRC = $(patsubst %.proto, %.pb.cc, $(PROTO_FILE))
PROTO_HEADER = $(patsubst %.proto, %.pb.h, $(PROTO_FILE))
PROTO_OBJ = $(patsubst %.cc, %.o, $(PROTO_SRC))

FORMAT_FILE_TYPES_SRC = $(wildcard src/common/format/*.cc) \
						src/proto/sortfile.pb.cc src/common/fileformat.cc
FILE_TYPES_SRC = src/common/file.cc $(wildcard src/common/file/*.cc)
SCANNER_SUPPORT_SRC = src/common/scanner.cc $(FORMAT_FILE_TYPES_SRC) $(FILE_TYPES_SRC)

MASTER_SRC = $(wildcard src/master/*.cc) \
			 $(filter-out src/proto/sortfile.pb.cc, $(PROTO_SRC)) $(SCANNER_SUPPORT_SRC) \
			 src/common/dag_scheduler.cc
MASTER_OBJ = $(patsubst %.cc, %.o, $(MASTER_SRC))

MINION_SRC = $(wildcard src/minion/container/*.cc) \
			 src/proto/shuttle.pb.cc src/proto/minion.pb.cc src/proto/master.pb.cc \
			 $(FILE_TYPES_SRC) $(FORMAT_FILE_TYPES_SRC) \
			 src/common/dag_scheduler.cc
MINION_OBJ = $(patsubst %.cc, %.o, $(MINION_SRC))

INLET_SRC = $(wildcard src/minion/input/*.cc) \
			src/proto/shuttle.pb.cc $(SCANNER_SUPPORT_SRC)
INLET_OBJ = $(patsubst %.cc, %.o, $(INLET_SRC))

COMBINER_SRC = $(wildcard src/minion/combiner/*.cc) \
			   src/proto/shuttle.pb.cc \
			   $(FILE_TYPES_SRC) $(FORMAT_FILE_TYPES_SRC) \
			   src/minion/common/streaming.cc src/minion/common/emitter.cc \
			   src/minion/output/partition.cc
COMBINER_OBJ = $(patsubst %.cc, %.o, $(COMBINER_SRC))

OUTLET_SRC = $(wildcard src/minion/output/*.cc) \
			 src/proto/shuttle.pb.cc \
			 $(FILE_TYPES_SRC) $(FORMAT_FILE_TYPES_SRC) \
			 src/minion/common/streaming.cc src/minion/common/emitter.cc
OUTLET_OBJ = $(patsubst %.cc, %.o, $(OUTLET_SRC))

TEST_FILE_SRC = src/test/file.test.cc src/proto/shuttle.pb.cc \
				$(FILE_TYPES_SRC)
TEST_FILE_OBJ = $(patsubst %.cc, %.o, $(TEST_FILE_SRC))

TEST_FILE_FORMAT_SRC = src/test/fileformat.test.cc \
					   $(FILE_TYPES_SRC) $(FORMAT_FILE_TYPES_SRC)
TEST_FILE_FORMAT_OBJ = $(patsubst %.cc, %.o, $(TEST_FILE_FORMAT_SRC))

TEST_SCANNER_SRC = src/test/scanner.test.cc \
				   $(SCANNER_SUPPORT_SRC)
TEST_SCANNER_OBJ = $(patsubst %.cc, %.o, $(TEST_SCANNER_SRC))

TEST_MERGER_SRC = src/test/merger.test.cc src/minion/input/merger.cc \
				  src/proto/shuttle.pb.cc $(SCANNER_SUPPORT_SRC)
TEST_MERGER_OBJ = $(patsubst %.cc, %.o, $(TEST_MERGER_SRC))

TEST_DAG_SCHEDULER_SRC = src/test/dag_scheduler.test.cc \
						 src/common/dag_scheduler.cc \
						 src/proto/shuttle.pb.cc
TEST_DAG_SCHEDULER_OBJ = $(patsubst %.cc, %.o, $(TEST_DAG_SCHEDULER_SRC))

TEST_RESOURCE_MANAGER_SRC = src/test/resource_manager.test.cc \
							src/master/resource_manager.cc src/master/master_flags.cc \
							src/proto/shuttle.pb.cc $(SCANNER_SUPPORT_SRC)
TEST_RESOURCE_MANAGER_OBJ = $(patsubst %.cc, %.o, $(TEST_RESOURCE_MANAGER_SRC))

TOOL_PARTITION_SRC = src/tool/partition_tool.cc src/minion/output/partition.cc \
					 src/proto/shuttle.pb.cc
TOOL_PARTITION_OBJ = $(patsubst %.cc, %.o, $(TOOL_PARTITION_SRC))

TOOL_SORTFILE_SRC = src/tool/sortfile_tool.cc src/minion/input/merger.cc \
					src/proto/shuttle.pb.cc $(SCANNER_SUPPORT_SRC)
TOOL_SORTFILE_OBJ = $(patsubst %.cc, %.o, $(TOOL_SORTFILE_SRC))

TOOL_QUERY_SRC = src/tool/query_tool.cc src/proto/minion.pb.cc src/proto/shuttle.pb.cc
TOOL_QUERY_OBJ = $(patsubst %.cc, %.o, $(TOOL_QUERY_SRC))

LIB_SDK_SRC = $(wildcard src/sdk/*.cc)
LIB_SDK_OBJ = $(patsubst %.cc, %.o, $(LIB_SDK_SRC))

CLIENT_SRC = $(wildcard src/client/*.cc) \
			 src/proto/shuttle.pb.cc src/proto/master.pb.cc \
			 src/common/table_printer.cc
CLIENT_OBJ = $(patsubst %.cc, %.o, $(CLIENT_SRC))

OBJS = $(MASTER_OBJ) $(MINION_OBJ) $(INLET_OBJ) $(COMBINER_OBJ) $(OUTLET_OBJ) \
	   $(TEST_FILE_OBJ) $(TEST_FILE_FORMAT_OBJ) $(TEST_SCANNER_OBJ) $(TEST_MERGER_OBJ) \
	   $(TEST_DAG_SCHEDULER_OBJ) $(TEST_RESOURCE_MANAGER_OBJ) \
	   $(TOOL_PARTITION_OBJ) $(TOOL_SORTFILE_OBJ) $(TOOL_QUERY_OBJ) \
	   $(LIB_SDK_OBJ) $(CLIENT_OBJ)
BIN = master minion inlet combiner outlet phaser tricorder beam shuttle-internal
TESTS = file_test fileformat_test scanner_test merger_test dag_test rm_test
LIB = libshuttle.a
DEPS = $(patsubst %.o, %.d, $(OBJS))

# Default build all binary files except tests
all: $(BIN) $(LIB)
	@echo 'make all done.'

# Dependencies
$(OBJS): $(PROTO_HEADER)
-include $(DEPS)

# Building targets
%.pb.cc %.pb.h: %.proto
	$(PROTOC) --proto_path=./src/proto --cpp_out=./src/proto $<

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(INCPATH) -c $< -o $@

test: $(TESTS)
	@echo 'make test done'

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
	$(CXX) $(TEST_FILE_OBJ) -o $@ $(LDFLAGS) $(TESTFLAGS)

fileformat_test: $(TEST_FILE_FORMAT_OBJ)
	$(CXX) $(TEST_FILE_FORMAT_OBJ) -o $@ $(LDFLAGS) $(TESTFLAGS)

scanner_test: $(TEST_SCANNER_OBJ)
	$(CXX) $(TEST_SCANNER_OBJ) -o $@ $(LDFLAGS) $(TESTFLAGS)

merger_test: $(TEST_MERGER_OBJ)
	$(CXX) $(TEST_MERGER_OBJ) -o $@ $(LDFLAGS) $(TESTFLAGS)

dag_test: $(TEST_DAG_SCHEDULER_OBJ)
	$(CXX) $(TEST_DAG_SCHEDULER_OBJ) -o $@ $(LDFLAGS) $(TESTFLAGS)

rm_test: $(TEST_RESOURCE_MANAGER_OBJ)
	$(CXX) $(TEST_RESOURCE_MANAGER_OBJ) -o $@ $(LDFLAGS) $(TESTFLAGS)

phaser: $(TOOL_PARTITION_OBJ)
	$(CXX) $(TOOL_PARTITION_OBJ) -o $@ \
		-L$(GALAXY_DIR)/thirdparty/lib -lgflags -lprotobuf -lpthread

tricorder: $(TOOL_SORTFILE_OBJ)
	$(CXX) $(TOOL_SORTFILE_OBJ) -o $@ $(LDFLAGS)

beam: $(TOOL_QUERY_OBJ)
	$(CXX) $(TOOL_QUERY_OBJ) -o $@ $(BASIC_LD_FLAGS)

libshuttle.a: $(LIB_SDK_OBJ)
	ar crs $@ $(LIB_SDK_OBJ)

shuttle-internal: libshuttle.a $(CLIENT_OBJ)
	$(CXX) $(CLIENT_OBJ) -o $@ -L. -lshuttle $(BASIC_LD_FLAGS) \
		-L$(BOOST_DIR)/stage/lib -lboost_program_options

.PHONY: clean install output
clean:
	@rm -rf output/
	@rm -rf $(BIN) $(LIB) $(TESTS) $(OBJS) $(DEPS)
	@rm -rf $(PROTO_SRC) $(PROTO_HEADER)
	@echo 'make clean done'

output: $(BIN)
	mkdir -p output/bin
	cp $(BIN) output/bin

