# Modify prefix to specify the directory to install shuttle
PREFIX = ./output

# Include dependency path information in depends.mk
include depends.mk

# Compiler related
OPT ?= -O2 -g2
CXX = g++
INCPATH = -I. -I./src -I$(BOOST_DIR) -I$(LIB_HDFS_DIR)/output/include \
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
PROTO_FILE = $(wildcard proto/*.proto)
PROTO_SRC = $(patsubst %.proto, %.pb.cc, $(PROTO_FILE))
PROTO_HEADER = $(patsubst %.proto, %.pb.h, $(PROTO_FILE))
PROTO_OBJ = $(patsubst %.cc, %.o, $(PROTO_SRC))

MASTER_SRC = $(filter-out %_test.cc, $(wildcard src/master/*.cc)) \
			 $(PROTO_SRC) \
			 src/common/filesystem.cc src/common/tools_util.cc \
			 src/sort/input_reader.cc src/sort/sort_file_impl.cc
MASTER_OBJ = $(patsubst %.cc, %.o, $(MASTER_SRC))

MINION_SRC = $(filter-out %_test.cc %_tool.cc, $(wildcard src/minion/*.cc)) \
			 $(PROTO_SRC) \
			 src/common/filesystem.cc src/common/tools_util.cc \
			 src/common/net_statistics.cc src/sort/sort_file_impl.cc
MINION_OBJ = $(patsubst %.cc, %.o, $(MINION_SRC))

INPUT_READER_SRC = proto/shuttle.pb.cc src/sort/input_reader.cc \
				   src/common/filesystem.cc src/common/tools_util.cc
SORT_FILE_SRC = proto/sortfile.pb.cc proto/shuttle.pb.cc \
				src/sort/sort_file_impl.cc \
				src/common/filesystem.cc src/common/tools_util.cc

INPUT_TOOL_SRC = src/sort/input_tool.cc $(INPUT_READER_SRC)
INPUT_TOOL_OBJ = $(patsubst %.cc, %.o, $(INPUT_TOOL_SRC))

SHUFFLE_TOOL_SRC = src/sort/shuffle_tool.cc src/sort/merge_file_impl.cc \
				   $(SORT_FILE_SRC)
SHUFFLE_TOOL_OBJ = $(patsubst %.cc, %.o, $(SHUFFLE_TOOL_SRC))

TUO_MERGER_SRC = src/sort/tuo_merger.cc src/sort/merge_file_impl.cc \
				 $(SORT_FILE_SRC)
TUO_MERGER_OBJ = $(patsubst %.cc, %.o, $(TUO_MERGER_SRC))

COMBINE_TOOL_SRC = src/sort/combine_tool.cc src/sort/merge_file_impl.cc \
					src/minion/partition.cc $(SORT_FILE_SRC)
COMBINE_TOOL_OBJ = $(patsubst %.cc, %.o, $(COMBINE_TOOL_SRC))

TEST_SORT_SRC = src/sort/sort_file_hdfs_test.cc $(SORT_FILE_SRC)
TEST_SORT_OBJ = $(patsubst %.cc, %.o, $(TEST_SORT_SRC))

TOOL_SORT_FILE_SRC = src/sort/sf_tool.cc src/sort/merge_file_impl.cc \
					 $(SORT_FILE_SRC)
TOOL_SORT_FILE_OBJ = $(patsubst %.cc, %.o, $(TOOL_SORT_FILE_SRC))

TOOL_PARTITION_SRC = src/minion/partition_tool.cc src/minion/partition.cc \
					 proto/shuttle.pb.cc
TOOL_PARTITION_OBJ = $(patsubst %.cc, %.o, $(TOOL_PARTITION_SRC))

LIB_SDK_SRC = $(wildcard src/sdk/*.cc) \
			  proto/app_master.pb.cc proto/shuttle.pb.cc
LIB_SDK_OBJ = $(patsubst %.cc, %.o, $(LIB_SDK_SRC))

CLIENT_SRC = $(wildcard src/client/*.cc) \
			 src/common/table_printer.cc
CLIENT_OBJ = $(patsubst %.cc, %.o, $(CLIENT_SRC))

OBJS = $(MASTER_OBJ) $(MINION_OBJ) $(INPUT_TOOL_OBJ) $(SHUFFLE_TOOL_OBJ) \
	   $(TUO_MERGER_OBJ) $(COMBINE_TOOL_OBJ) $(LIB_SDK_OBJ) $(CLIENT_OBJ)\
	   $(TEST_SORT_OBJ) \
	   $(TOOL_SORT_FILE_OBJ) $(TOOL_PARTITION_OBJ)
BIN = master minion input_tool shuffle_tool tuo_merger combine_tool sf_tool partition_tool
TESTS = sort_test
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
	$(PROTOC) --proto_path=./proto --cpp_out=./proto $<

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(INCPATH) -c $< -o $@

test: $(TESTS)
	@echo 'make test done'

master: $(MASTER_OBJ)
	$(CXX) $(MASTER_OBJ) -o $@ $(LDFLAGS)

minion: $(MINION_OBJ)
	$(CXX) $(MINION_OBJ) -o $@ $(LDFLAGS)

input_tool: $(INPUT_TOOL_OBJ)
	$(CXX) $(INPUT_TOOL_OBJ) -o $@ $(LDFLAGS)

shuffle_tool: $(SHUFFLE_TOOL_OBJ)
	$(CXX) $(SHUFFLE_TOOL_OBJ) -o $@ $(LDFLAGS)

tuo_merger: $(TUO_MERGER_OBJ)
	$(CXX) $(TUO_MERGER_OBJ) -o $@ $(LDFLAGS)

combine_tool: $(COMBINE_TOOL_OBJ)
	$(CXX) $(COMBINE_TOOL_OBJ) -o $@ $(LDFLAGS)

sf_tool: $(TOOL_SORT_FILE_OBJ)
	$(CXX) $(TOOL_SORT_FILE_OBJ) -o $@ $(LDFLAGS)

partition_tool: $(TOOL_PARTITION_OBJ)
	$(CXX) $(TOOL_PARTITION_OBJ) -o $@ $(LDFLAGS)

libshuttle.a: $(LIB_SDK_OBJ)
	ar crs $@ $(LIB_SDK_OBJ)

shuttle-internal: libshuttle.a $(CLIENT_OBJ)
	$(CXX) $(CLIENT_OBJ) -o $@ -L. -lshuttle $(BASIC_LD_FLAGS)

.PHONY: clean install output
clean:
	@rm -rf output/
	@rm -rf $(BIN) $(LIB) $(TESTS) $(OBJS) $(DEPS)
	@rm -rf $(PROTO_SRC) $(PROTO_HEADER)
	@echo 'make clean done'

output: $(BIN)
	mkdir -p output/bin
	cp $(BIN) output/bin

