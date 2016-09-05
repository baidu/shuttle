#include "master/dag_scheduler.h"

#include <gtest/gtest.h>

#include <set>
#include <algorithm>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <cstdio>
#include <ctime>
#include <cstring>

using namespace baidu::shuttle;

class DagSchedulerTest : public DagScheduler {
public:
    virtual ~DagSchedulerTest() { }

    // Inner data validity test
    void TestIndegree();
    void TestMapValidity();

    static DagSchedulerTest* BuildDagSchedulerTest(const std::vector< std::vector<int> > map);

    static void GenerateUserDefinedMap(FILE* source, std::vector< std::vector<int> >& map);
private:
    DagSchedulerTest(const JobDescriptor& job) : DagScheduler(job) { }
};

// Test pointer to validate inner data structure
static DagSchedulerTest* test = NULL;
// Stores global wide map data
static std::vector< std::vector<int> > global_map;

DagSchedulerTest* DagSchedulerTest::BuildDagSchedulerTest(
        const std::vector< std::vector<int> > map) {
    JobDescriptor job;
    for (size_t i = 0; i < map.size(); ++i) {
        JobDescriptor_NodeNeigbor* cur_node = job.add_map();
        for (size_t j = 0; j < map[i].size(); ++j) {
            if (map[i][j] != 0) {
                cur_node->add_next(j);
            }
        }
    }
    return new DagSchedulerTest(job);
}

void DagSchedulerTest::GenerateUserDefinedMap(FILE* source, std::vector< std::vector<int> >& map) {
    char buf[1024] = { 0 };
    puts("Getting User Defined Map:");
    puts("  Please input the number of nodes:");
    fgets(buf, 1024, source);
    std::string node_line = buf;
    boost::algorithm::trim(node_line);

    int node = boost::lexical_cast<int>(node_line);
    map.resize(node);
    for (int i = 0; i < node; ++i) {
        map[i].resize(node);
    }

    puts("  Please input a square matrix represented a DAG. Begin");
    for (int i = 0; i < node; ++i) {
        fgets(buf, 1024, source);
        std::string line = buf;
        boost::algorithm::trim(line);
        std::vector<std::string> nexts;
        boost::split(nexts, line, boost::is_any_of(" "));
        for (size_t j = 0; j < nexts.size(); ++j) {
            try {
                map[i][j] = boost::lexical_cast<int>(nexts[j]);
            } catch (const boost::bad_lexical_cast&) {
                map[i][j] = 0;
            }
        }
    }
    puts("  Got it.");
}

void DagSchedulerTest::TestIndegree() {
    ASSERT_EQ(dependency_map_.size(), indegree_.size());
    for (size_t i = 0; i < dependency_map_.size(); ++i) {
        EXPECT_EQ(static_cast<int>(dependency_map_[i].pre.size()), indegree_[i]);
    }
    EXPECT_EQ(left_, static_cast<int>(indegree_.size()));
}

void DagSchedulerTest::TestMapValidity() {
    for (std::vector<DagNode>::iterator it = dependency_map_.begin();
            it != dependency_map_.end(); ++it) {
        for (std::vector<int>::iterator pre_it = it->pre.begin();
                pre_it != it->pre.end(); ++pre_it) {
            const std::vector<int>& pre_next = dependency_map_[*pre_it].next;
            EXPECT_NE(std::find(pre_next.begin(), pre_next.end(), it->node), pre_next.end());
        }
        for (std::vector<int>::iterator next_it = it->next.begin();
                next_it != it->next.end(); ++next_it) {
            const std::vector<int>& next_pre = dependency_map_[*next_it].pre;
            EXPECT_NE(std::find(next_pre.begin(), next_pre.end(), it->node), next_pre.end());
        }
    }
}

class DagSchedulerUnitTest : public testing::Test {
protected:
    virtual void SetUp() {
        if (test != NULL) {
            return;
        }
        ASSERT_TRUE(!global_map.empty());
        test = DagSchedulerTest::BuildDagSchedulerTest(global_map);
    }

    virtual void TearDown() {
        if (test != NULL) {
            delete test;
        }
        test = NULL;
    }
};

TEST_F(DagSchedulerUnitTest, InnerValidityTest) {
    test->TestMapValidity();
    test->TestIndegree();
    ASSERT_TRUE(test->Validate());
}

TEST_F(DagSchedulerUnitTest, CompletenessTest) {
    int last_unfinished = 0;
    while ((last_unfinished = test->UnfinishedNodes()) != 0) {
        const std::vector<int>& available = test->AvailableNodes();
        printf("Available: %d\n", (int)available.size());
        for (std::vector<int>::const_iterator it = available.begin();
                it != available.end(); ++it) {
            printf("Removing %d\n", *it);
            test->RemoveFinishedNode(*it);
        }
        printf("Unfinished: %d\n", test->UnfinishedNodes());
        if (test->UnfinishedNodes() == last_unfinished) {
            break;
        }
    }
    EXPECT_EQ(test->UnfinishedNodes(), 0);
}

TEST_F(DagSchedulerUnitTest, FunctionalityTest) {
    std::vector<int> available = test->AvailableNodes();
    std::vector<int> sources = test->Sources();
    std::sort(available.begin(), available.end());
    std::sort(sources.begin(), sources.end());
    EXPECT_EQ(available, sources);

    std::set<int> next_nodes;
    for (std::vector<int>::const_iterator it = available.begin();
            it != available.end(); ++it) {
        const std::vector<int>& next = test->NextNodes(*it);
        for (std::vector<int>::const_iterator jt = next.begin();
                jt != next.end(); ++jt) {
            printf("%d has successor %d\n", *it, *jt);
            next_nodes.insert(*jt);
        }
        printf("Removing %d\n", *it);
        test->RemoveFinishedNode(*it);
    }
    std::vector<int> next_vec;
    next_vec.resize(next_nodes.size());
    std::copy(next_nodes.begin(), next_nodes.end(), next_vec.begin());
    std::sort(next_vec.begin(), next_vec.end());
    puts("Removed nodes has following successors:");
    for (std::set<int>::iterator it = next_nodes.begin();
            it != next_nodes.end(); ++it) {
        printf("%d ", *it);
    }
    putchar('\n');
    std::vector<int> available_now = test->AvailableNodes();
    std::sort(available_now.begin(), available_now.end());
    puts("Now available:");
    for (std::vector<int>::iterator it = available_now.begin();
            it != available_now.end(); ++it) {
        printf("%d ", *it);
    }
    putchar('\n');
    ASSERT_TRUE(available_now.size() <= next_vec.size());
    
    size_t i = 0, j = 0;
    while (i < available_now.size()) {
        if (available_now[i] == next_vec[j]) {
            ++j;
            ++i;
        } else {
            // Find next available item in next_vec
            while (++j < next_vec.size() && next_vec[j] < available_now[i]);
            // Since next_vec must contains all items in available_now,
            //   so j must not reach the end before i does
            ASSERT_TRUE(j < next_vec.size());
            ASSERT_TRUE(next_vec[j] == available_now[i]);
        }
    }
}

int main(int argc, char** argv) {
    FILE* fp = NULL;
    if (argc == 2) {
        fp = fopen(argv[1], "r");
    }
    if (fp == NULL) {
        puts("cannot find valid map file, use stdin instead");
        fp = stdin;
    }
    DagSchedulerTest::GenerateUserDefinedMap(fp, global_map);
    if (fp != stdin) {
        fclose(fp);
    }
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

