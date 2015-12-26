#include "dag_scheduler.h"

#include <algorithm>
#include <gtest/gtest.h>
#include <cstdlib>
#include <ctime>

using namespace baidu::shuttle;

class DagSchedulerTest : public DagScheduler {
public:
    virtual ~DagSchedulerTest() { }

    // Inner data validity test
    void TestIndegree();
    void TestMapValidity();

    static DagSchedulerTest* BuildDagSchedulerTest(const std::vector< std::vector<int> > map);

    static void GenerateRandomMap(std::vector< std::vector<int> >& map);
private:
    DagSchedulerTest(const JobDescriptor& job) : DagScheduler(job) { }
};

TEST(DagSchedulerTest, InnerValidityTest) {
    std::vector< std::vector<int> > map;
    DagSchedulerTest::GenerateRandomMap(map);
    DagSchedulerTest* test = DagSchedulerTest::BuildDagSchedulerTest(map);
    test->TestMapValidity();
    test->TestIndegree();
}

TEST(DagSchedulerTest, FunctionalityTest) {
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

// ----- Implementations of DagSchedulerTest -----
DagSchedulerTest* DagSchedulerTest::BuildDagSchedulerTest(
        const std::vector< std::vector<int> > map) {
    JobDescriptor job;
    for (std::vector< std::vector<int> >::const_iterator it = map.begin();
            it != map.end(); ++it) {
        JobDescriptor_NodeNeigbor* cur = job.add_map();
        cur->mutable_next()->Reserve(it->size());
        std::copy(it->begin(), it->end(), cur->mutable_next()->begin());
    }
    return new DagSchedulerTest(job);
}

void DagSchedulerTest::GenerateRandomMap(std::vector< std::vector<int> >& /*map*/) {
    srand(time(NULL));
    // Test map should not have more than 20 nodes
    // TODO Generate DAG
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

