#include "gtest/gtest.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <algorithm>
#include "project_path.h"
#include "map_reduce_runner.h"
#include "prefix_functors.h"

using namespace std::string_literals;

struct TestParams {
    std::string in_file;
    int num_threads_map;
    int num_threads_reduce;
    int expected_result;
};

class AssignmentTestFromFile : public testing::TestWithParam<TestParams> {
};

TEST_P(AssignmentTestFromFile, AssignmentExample) {
    auto task_runner = mapreduce::MapReduceRunner<
            prefix::PrefixMapper,
            prefix::PrefixReducer>(
            GetParam().in_file,
            GetParam().num_threads_map,
            GetParam().num_threads_reduce);
    std::vector<int> reduce_results = task_runner.process();
    auto result = std::max_element(reduce_results.cbegin(), reduce_results.cend());
    ASSERT_EQ(*result, GetParam().expected_result);
}


INSTANTIATE_TEST_CASE_P(MyGroup, AssignmentTestFromFile, ::testing::Values(
        TestParams{PROJECT_SOURCE_DIR + "/test/data/test.1.in.txt"s, 1, 1, 8},
        TestParams{PROJECT_SOURCE_DIR + "/test/data/test.2.in.txt"s, 1, 1, 12},
        TestParams{PROJECT_SOURCE_DIR + "/test/data/test.3.in.txt"s, 1, 1, 13}
));

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
