#include <iostream>
#include <sstream>
#include <fstream>
#include <string>

bool file_exists(const std::string& file_name) {
    std::ifstream infile(file_name);
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

    return 0;
}
