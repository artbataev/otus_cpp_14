#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <thread>
#include <chrono>
#include <algorithm>
#include <cassert>

namespace mapreduce {
    template<typename MapCls, typename ReduceCls>
    class MapReduceRunner {
    public:
        using map_result_t = typename decltype(std::declval<MapCls>()("", ""))::value_type; // vector -> value_type
        using map_value_t = typename decltype(std::declval<MapCls>()("", ""))::value_type::second_type;
        // vector<pair> -> second_type is value
        using reduce_result_t = decltype(std::declval<ReduceCls>()("", {}));


        MapReduceRunner(
                std::string filename_,
                int num_threads_map_,
                int num_threads_reduce_,
                std::string path_to_save_reduce_files_ = "") :
                filename(std::move(filename_)),
                num_threads_map(num_threads_map_),
                num_threads_reduce(num_threads_reduce_),
                path_to_save_reduce_files(std::move(path_to_save_reduce_files_)),
                map_results(static_cast<size_t>(num_threads_map_)) {}


        std::vector<reduce_result_t> process() {
            run_map();
            run_shuffle();
            return run_reduce();
        }

    private:
        int num_threads_map;
        int num_threads_reduce;
        const std::string filename;
        const std::string path_to_save_reduce_files;
        std::vector<map_result_t> intermediate_data;
        std::vector<std::vector<map_result_t>> map_results;
        std::vector<int> lines_indices;


        void run_map() {
            std::ifstream infile(filename);
            std::string buf;
            lines_indices.push_back(0);
            while (std::getline(infile, buf))
                lines_indices.emplace_back(infile.tellg());
            infile.close();

            auto total_blocks = static_cast<int>(lines_indices.size()) - 1;
            auto num_threads = std::min(num_threads_map, total_blocks);
            std::vector<std::thread> map_threads;
            for (int i = 0, j = 0, j_next = 0, used_blocks = 0; i < num_threads; i++) {
                j = j_next;
                j_next = j_next + (total_blocks - used_blocks) / (num_threads - i); // TODO: test
                map_threads.emplace_back([this, j, j_next, i] {
                    this->read_file_block(lines_indices[j], lines_indices[j_next], i);
                });
                used_blocks += j_next - j;
            }
            for (auto& t: map_threads)
                t.join();
        }

        struct ReducerData {
            std::string key;
            std::vector<map_value_t> values;
        };


        std::vector<reduce_result_t> run_reduce() {
            ReduceCls reducer(path_to_save_reduce_files + "reduce_1.txt");
            // TODO: multithreading
            std::vector<ReducerData> buffer;
            int last_j = -1;
            for (const auto& elem: intermediate_data) {
                if (last_j == -1 || buffer[last_j].key != elem.first) {
                    buffer.push_back({elem.first, {elem.second}});
                    last_j++;
                } else {
                    buffer[last_j].values.emplace_back(elem.second);
                }
            }
            int result = 0;
            for (const auto& elem: buffer)
                result = reducer(elem.key, elem.values);

            return {result};
        }


        // merge sort for sorted map results
        std::vector<map_result_t> merge_map_results(int i, int j) {
            assert(i <= j);
            if (i == j)
                return map_results[i];

            std::vector<map_result_t> result1;
            std::vector<map_result_t> result2;
            if (i + 1 == j) {
                result1 = map_results[i];
                result2 = map_results[j];
            } else {
                int middle = (i + j) / 2;
                result1 = merge_map_results(i, middle);
                result2 = merge_map_results(middle + 1, j);
            }

            std::vector<map_result_t> result{};
            result.reserve(result1.size() + result2.size());

            int i1 = 0;
            int i2 = 0;
            while (i1 < result1.size() || i2 < result2.size()) {
                if (i1 >= result1.size())
                    result.emplace_back(result2[i2++]);
                else if (i2 >= result2.size())
                    result.emplace_back(result1[i1++]);
                else if (result1[i1] < result2[i1])
                    result.emplace_back(result1[i1++]);
                else
                    result.emplace_back(result2[i2++]);
            }

            return result;
        };


        void run_shuffle() {
            intermediate_data = merge_map_results(0, map_results.size() - 1);
        };


        void read_file_block(int i_start, int i_end, int container_idx) {
            std::ifstream file(filename);
            file.seekg(i_start);
            std::string current_email;
            MapCls map_func{};
            while (file.tellg() < i_end && (file >> current_email)) {
                if (file.tellg() <= i_end) { // additional check boundaries
                    auto map_result = map_func(filename, current_email);
                    map_results[container_idx].reserve(map_results[container_idx].size() + map_result.size());
                    std::move(map_result.begin(), map_result.end(), std::back_inserter(map_results[container_idx]));
                }
            }
            file.close();
            std::sort(map_results[container_idx].begin(), map_results[container_idx].end());
        }
    };
}
