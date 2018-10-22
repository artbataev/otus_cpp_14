#pragma once

#include <fstream>
#include <vector>
#include <string>

namespace prefix {
    using mapper_key_t = std::string;
    using mapper_value_t = std::pair<int, std::string>;
    using reducer_value_t = int;

    class PrefixMapper {
    public:
        using kv_t = std::pair<mapper_key_t, mapper_value_t>;

        std::vector<kv_t> operator()(const std::string& key, const std::string& email) {
            std::vector<kv_t> result;
            if (!email.empty()) {
                result.reserve(email.size());
                for (size_t n_prefix_elements = 1; n_prefix_elements <= email.size(); n_prefix_elements++) {
                    result.emplace_back(
                            std::make_pair(email.substr(0, n_prefix_elements),
                                           std::make_pair(1, email))
                    );
                }
            }
            return result;
        }
    };

    class PrefixReducer {
    public:
        explicit PrefixReducer(const std::string& filename) : f(filename), result(0) {};

        int operator()(const mapper_key_t& key, const std::vector<mapper_value_t>& values) {
            f << key << "\n";
            if (values.size() > 1) {
                bool all_emails_equal = true;
                for(int i = 1; i < values.size(); i++)
                    if (values[i].second != values[i-1].second) {
                        all_emails_equal = false;
                        break;
                    }
                int current_result = static_cast<int>(key.size()) + 1;
                if(all_emails_equal)
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
