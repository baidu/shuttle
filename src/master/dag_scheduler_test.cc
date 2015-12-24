#include "dag_scheduler.h"

#include <gtest/gtest.h>

using namespace baidu::shuttle;

class DagSchedulerTest : public DagScheduler {
public:
    DagSchedulerTest(const std::vector< std::vector<int> > map);
    virtual ~DagSchedulerTest();

    // Inner data validity test
    int TestIndegree();
    int TestMapValidity();
};

TEST(DagSchedulerTest, InnerValidityTest) {
}

TEST(DagSchedulerTest, FunctionalityTest) {
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

// ----- Implementations of DagSchedulerTest -----

int DagSchedulerTest::TestIndegree() {
    return 0;
}

int DagSchedulerTest::TestMapValidity() {
    return 0;
}

