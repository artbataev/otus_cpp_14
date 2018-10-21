#pragma once

#include <fstream>
#include <vector>
#include <string>

namespace prefix {
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
}
