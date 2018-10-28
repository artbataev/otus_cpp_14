#pragma once

#include <fstream>
#include <vector>
#include <string>

namespace prefix {
    // implements original algorithm from Assignment
    // Mapper:
    // - take email as value
    // - return list of pairs <prefix, email> for all prefixes
    // - NB: we can't return list of pairs <prefix, email>, because we will loose information about duplicated emails
    // but for duplicated emails result should be 1
    // Reducer:
    // takes key (prefix) and list of values (emails)
    // if number of values > 1 => result should be size(prefix) + 1,
    // but if all values are equal, result is 1 (here we need original emails)

    using mapper_key_t = std::string;
    using mapper_value_t = std::string;
    using reducer_value_t = int;

    class PrefixMapper {
    public:
        using kv_t = std::pair<mapper_key_t, mapper_value_t>;

        std::vector<kv_t> operator()(const std::string& key, const std::string& email) {
            std::vector<kv_t> result;
            if (!email.empty()) {
                result.reserve(email.size());
                for (size_t n_prefix_elements = 1; n_prefix_elements <= email.size(); n_prefix_elements++) {
                    result.emplace_back(email.substr(0, n_prefix_elements), email);
                }
            }
            return result;
        }
    };

    class PrefixReducer {
    public:
        explicit PrefixReducer(const std::string& filename) : f(filename), result(1) {};

        int operator()(const mapper_key_t& key, const std::vector<mapper_value_t>& values) {
            f << key << "\n";
            if (values.size() > 1) {
                bool all_emails_equal = true;
                for (int i = 1; i < static_cast<int>(values.size()); i++)
                    if (values[i] != values[i - 1]) {
                        all_emails_equal = false;
                        break;
                    }
                int current_result = static_cast<int>(key.size()) + 1;
                if (all_emails_equal)
                    current_result = 1; // 1 - mininum, empty strings not used

                result = std::max(result, current_result);
            }
            return result;
        }

    private:
        std::ofstream f;
        reducer_value_t result;
    };
}


namespace prefix_no_duplicates {
    // implements original algorithm from Assignment when there are no duplicated emails
    // Mapper:
    // - take email as value
    // - return list of pairs <prefix, 1> for all prefixes
    // - NB: when return list of pairs <prefix, 1>, there is no information about duplicated emails
    // Reducer:
    // takes key (prefix) and list of values (1)
    // if number of values > 1 => result should be size(prefix) + 1

    using mapper_key_t = std::string;
    using mapper_value_t = int;
    using reducer_value_t = int;

    class PrefixMapper {
    public:
        using kv_t = std::pair<mapper_key_t, mapper_value_t>;

        std::vector<kv_t> operator()(const std::string& key, const std::string& email) {
            std::vector<kv_t> result;
            if (!email.empty()) {
                result.reserve(email.size());
                for (size_t n_prefix_elements = 1; n_prefix_elements <= email.size(); n_prefix_elements++) {
                    result.emplace_back(email.substr(0, n_prefix_elements), 1);
                }
            }
            return result;
        }
    };

    class PrefixReducer {
    public:
        explicit PrefixReducer(const std::string& filename) : f(filename), result(1) {};

        int operator()(const mapper_key_t& key, const std::vector<mapper_value_t>& values) {
            f << key << "\n";
            if (values.size() > 1)
                result = std::max(result, static_cast<int>(key.size()) + 1);

            return result;
        }

    private:
        std::ofstream f;
        reducer_value_t result;
    };
}

namespace prefix_optimized {
    // implements optimized algorithm
    // Mapper:
    // - take email as value
    // - return list of pairs <first letter, email>
    // Reducer:
    // takes key (first letter) and list of values (emails)
    // sorts them and finds shortest prefix to identify all emails

    using mapper_key_t = std::string;
    using mapper_value_t = std::string;
    using reducer_value_t = int;

    class PrefixMapper {
    public:
        using kv_t = std::pair<mapper_key_t, mapper_value_t>;

        std::vector<kv_t> operator()(const std::string& key, const std::string& email) {
            std::vector<kv_t> result({std::make_pair(std::string{email[0]}, email)});
            return result;
        }
    };

    class PrefixReducer {
    public:
        explicit PrefixReducer(const std::string& filename) : f(filename), result(1) {};

        int operator()(const mapper_key_t& key, std::vector<mapper_value_t> values) {
            f << key << "\n";
            if (values.size() > 1) {
                std::sort(values.begin(), values.end());
                reducer_value_t current_result;
                for (int i = 1; i < static_cast<int>(values.size()); i++) {
                    const auto& prev = values[i - 1];
                    const auto& cur = values[i];
                    current_result = 1;
                    if (prev != cur) { // else 1
                        for (current_result = 1;
                             current_result <= static_cast<int>(std::min(prev.size(), cur.size()))
                             && prev[current_result - 1] == cur[current_result - 1];
                             current_result++) {}
                    }
                    result = std::max(result, current_result);
                }
            }
            return result;
        }

    private:
        std::ofstream f;
        reducer_value_t result;
    };
}
