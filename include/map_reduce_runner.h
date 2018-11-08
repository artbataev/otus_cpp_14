#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <thread>
#include <chrono>
#include <algorithm>
#include <future>
#include <limits>
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

        // main function: map + shuffle + reduce
        std::vector<reduce_result_t> process() {
            run_map();
            run_shuffle();
            run_reduce();
            return reduce_results;
        }

    private:
        int num_threads_map;
        int num_threads_reduce;

        const std::string filename;
        const std::string path_to_save_reduce_files;

        std::vector<int> lines_indices;

        std::vector<std::vector<map_result_t>> map_results;
        std::vector<map_result_t> map_results_flat;

        // usefull struct to prepare data for reduce
        struct ReducerData {
            std::string key;
            std::vector<map_value_t> values;
        };

        std::vector<ReducerData> data_for_reducer;
        std::vector<reduce_result_t> reduce_results;


        void run_map() {
            // reading file to get "\n" symbols position to split data for mapper
            std::ifstream infile(filename);
            std::string buf;
            lines_indices.push_back(0);
            while (std::getline(infile, buf)) {
                auto position = infile.tellg();
                if (position >= 0)
                    lines_indices.emplace_back(position);
                else
                    break;
            }
            infile.close();
            lines_indices.push_back(std::numeric_limits<int>::max());

            auto total_blocks = static_cast<int>(lines_indices.size()) - 1;
            auto num_threads = std::min(num_threads_map, total_blocks);
            double step = static_cast<double>(total_blocks) / num_threads; // step >= 1 always
            std::vector<std::thread> map_threads;
            for (int i = 0, j = 0, j_next = 0; i < num_threads; i++) {
                j = j_next;
                if (i == num_threads - 1)
                    j_next = total_blocks;
                else
                    j_next = static_cast<int>(step * (i + 1));

                map_threads.emplace_back([this, j, j_next, i] {
                    this->run_single_mapper(lines_indices[j], lines_indices[j_next], i);
                });
            }

            for (auto& t: map_threads)
                t.join();
        }


        // groups data by key for reducer and run in threads reduce function
        void run_reduce() {
            // grouping data (by key) for reducer: map_results_flat is sorted
            int last_j = -1;
            for (const auto& elem: map_results_flat) {
                if (last_j == -1 || data_for_reducer[last_j].key != elem.first) {
                    data_for_reducer.push_back({elem.first, {elem.second}});
                    last_j++;
                } else {
                    data_for_reducer[last_j].values.emplace_back(elem.second);
                }
            }

            auto num_threads = std::min(num_threads_reduce, static_cast<int>(data_for_reducer.size()));
            reduce_results = std::vector<reduce_result_t>(num_threads, reduce_result_t());

            std::vector<std::thread> reduce_threads;
            auto total_blocks = static_cast<int>(data_for_reducer.size());
            double step = static_cast<double>(total_blocks) / num_threads; // step >= 1
            for (int i = 0, j = 0, j_next = 0; i < num_threads; i++) {
                j = j_next;
                if (i == num_threads - 1)
                    j_next = total_blocks;
                else
                    j_next = static_cast<int>(step * (i + 1));
                reduce_threads.emplace_back([this, j, j_next, i] {
                    this->run_single_reducer(j, j_next, i);
                });
            }
            for (auto& t: reduce_threads)
                t.join();
        }

        // runs reducer
        // writes result for separated container, so there is no need to use mutex
        void run_single_reducer(int start_i, int end_i, int thread_idx) {
            ReduceCls reducer(path_to_save_reduce_files + "reduce_" + std::to_string(thread_idx) + ".txt");
            for (int i = start_i; i < end_i; i++)
                reduce_results[thread_idx] = reducer(data_for_reducer[i].key, data_for_reducer[i].values);
        }

        // async merge sort for sorted map results,
        // will work in not more than num_threads_map threads,
        // because: num_threads_map / 2 + num_threads_map / 4 + ... + 1 etc <= num_threads_map
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
                // result1 = merge_map_results(i, middle);
                std::future<std::vector<map_result_t>> result1_future = std::async(
                        std::launch::async,
                        [this, i, middle] {
                            return this->merge_map_results(i, middle);
                        });
                result2 = merge_map_results(middle + 1, j);
                result1 = result1_future.get();
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
                else if (result1[i1].first <= result2[i2].first)
                    result.emplace_back(result1[i1++]);
                else
                    result.emplace_back(result2[i2++]);
            }

            return result;
        };

        // sorting intermediate data
        void run_shuffle() {
            map_results_flat = merge_map_results(0, map_results.size() - 1);
        };


        // reads data from file and calls map function
        // writes to separated container: no need to use mutex
        void run_single_mapper(int i_start, int i_end, int container_idx) {
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
            std::stable_sort(map_results[container_idx].begin(), map_results[container_idx].end());
        }
    };
}
