#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <algorithm>
#include "map_reduce_runner.h"

class PrefixMapper {
    using kv_t = std::pair<std::string, std::string>;
public:
    std::vector<kv_t> operator()(const std::string& key, const std::string& email) {
        std::vector<kv_t> result;
        if (!email.empty()) {
            result.reserve(email.size());
            for (size_t n_prefix_elements = 1; n_prefix_elements <= email.size(); n_prefix_elements++) {
                result.emplace_back(std::make_pair(email.substr(0, n_prefix_elements), email));
            }
        }
        return result;
    }
};

class PrefixReducer {
public:
    explicit PrefixReducer(const std::string& filename) : f(filename), result(0) {};

    int operator()(const std::string& key, const std::vector<std::string>& values) {
        f << key << "\n";
        if (values.size() > 1)
            result = std::max(result, static_cast<int>(key.size()) + 1);
        return result;
    }

private:
    std::ofstream f;
    int result;
};


bool file_exists(const std::string& filename) {
    std::ifstream infile(filename);
    return infile.good();
}

int main(int argc, char *argv[]) {
    bool executed_correctly = true;
    std::stringstream whats_wrong;
    std::string src_file;
    int num_threads_map = 1;
    int num_threads_reduce = 1;


    if (argc != 4) {
        executed_correctly = false;
        whats_wrong << "Incorrect number of arguments";
    } else {
        try {
            src_file = argv[1];
            num_threads_map = std::stoi(argv[2]);
            num_threads_reduce = std::stoi(argv[3]);
            if (num_threads_map <= 0 or num_threads_reduce <= 0) {
                whats_wrong << "Number of threads must be positive";
                executed_correctly = false;
            } else if (!file_exists(src_file)) {
                whats_wrong << "File " << src_file << " doesn't exist!";
                executed_correctly = false;
            }
        } catch (std::exception& ex) {
            executed_correctly = false;
            whats_wrong << ex.what();
        }
    }

    if (!executed_correctly) {
        std::cout << "Error: " << whats_wrong.str() << std::endl;
        std::cout << "Execute with 3 arguments - source file, num threads for map, num threads for reduce, e.g.:"
                  << std::endl;
        std::cout << argv[0] << " infile.txt 4 4" << std::endl;
        exit(0);
    }

    auto task_runner = MapReduceRunner<PrefixMapper, PrefixReducer>(src_file, num_threads_map, num_threads_reduce);
    std::vector<int> reduce_results = task_runner.process();
    auto result = std::max_element(reduce_results.cbegin(), reduce_results.cend());
    if (result != reduce_results.cend()) {
        std::cout << *result << std::endl;
    } else {
        std::cout << "empty result" << std::endl;
    }

    return 0;
}
