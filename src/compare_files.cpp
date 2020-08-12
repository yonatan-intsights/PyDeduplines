#include <fstream>
#include <string>
#include <string_view>
#include <vector>
#include <iostream>
#include <filesystem>
#include <thread>
#include <exception>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "parallel_hashmap/phmap.h"

#define MAX_LINE_SIZE 1024

std::vector<char>* read_file(std::filesystem::path path);

void load_lines_from_file_to_set2(
    std::vector<char>& data,
    phmap::flat_hash_set<std::string_view> &lines_set
);

void load_lines_from_file_to_set(
    std::filesystem::path input_file_path,
    phmap::flat_hash_set<std::string_view> &lines_set
);

void check_file_lines_not_in_set(
    std::filesystem::path changed_file_path,
    phmap::flat_hash_set<std::string_view> &lines_set,
    std::vector<std::string> *result
);

void check_file_lines_not_in_set2(
    std::filesystem::path changed_file_path,
    phmap::flat_hash_set<std::string_view> &lines_set,
    std::vector<std::string> *result);


void compute_added_lines(
    std::filesystem::path original_file_path,
    std::filesystem::path changed_file_path,
    std::vector<std::string> *result
) {
    // std::cout << "original file: " << original_file_path;
    // std::cout << "changed file: " << changed_file_path;
    phmap::flat_hash_set<std::string_view> lines_set;
    // phmap::parallel_flat_hash_set<std::string_view> lines_set;
    // robin_hood::unordered_set<std::string_view> lines_set;

    std::vector<char> *buffer = read_file(original_file_path);

    load_lines_from_file_to_set2(*buffer, lines_set);

    // load_lines_from_file_to_set(original_file_path, lines_set);

    //std::cout << "finished loading to hash set\n";

    check_file_lines_not_in_set(
        changed_file_path,
        lines_set,
        result
    );

    //std::cout << "total new lines found: " << num_new_found << "\n";
}

void check_file_lines_not_in_set2(
    std::filesystem::path changed_file_path,
    phmap::flat_hash_set<std::string_view> &lines_set,
    std::vector<std::string> *result
) {
    int fd = open(changed_file_path.c_str(), O_RDONLY);

    if (fd == -1) {
        throw std::exception();
    }

    char all_buffer[128 * 1024 + MAX_LINE_SIZE];
    char* buffer = all_buffer + MAX_LINE_SIZE;
    char *buffer_end = buffer + (128 * 1024);

    // printf("buffer start: %ld\n", buffer);
    // printf("buffer end  : %ld\n", buffer_end);
    // printf("buffer size : %d\n", buffer_end - buffer);

    int num_read_bytes;
    int left_over_length = 0;
    while ( (num_read_bytes = read(fd, buffer, 128*1024)) > 0 ) {
        char *start = buffer - left_over_length;
        char* end;

        //printf("current file position: %ld\n", lseek(fd, 0, SEEK_CUR));

        while (start < buffer_end) {
            end = (char*)memchr(start, '\n', buffer_end - start);
            if (end == nullptr) {
                left_over_length = buffer_end - start;
                memcpy(buffer - left_over_length, start, left_over_length);
                break;
            } else {
                left_over_length = 0;
                std::string_view line_sv = std::string_view(start, end - start);
                bool contained = lines_set.contains(line_sv);
                if (!contained) {
                    //std::cout << "found added line: " << line_sv << std::endl;
                    result->push_back(std::string(line_sv));
                }
                start = end + 1;
            }
        }
    }
}

void check_file_lines_not_in_set(
    std::filesystem::path changed_file_path,
    phmap::flat_hash_set<std::string_view> &lines_set,
    std::vector<std::string> *result
)
{
    FILE *change_file = fopen(changed_file_path.c_str(), "r");
    if (!change_file)
    {
        std::cout << "file not opened: " << changed_file_path << std::endl;
        return;
    }

    int num_new_found = 0;

    char line_buf[MAX_LINE_SIZE] = {};

    size_t len_line;
    while (fgets(line_buf, MAX_LINE_SIZE, change_file))
    {
        len_line = strlen(line_buf) - 1;
        std::string_view line_sv(line_buf, len_line);

        bool contained = lines_set.contains(line_sv);
        if (!contained)
        {
            num_new_found += 1;

            //std::cout << "found added line: " << line_sv << std::endl;
            result->push_back(std::string(line_sv));
        }
    }
}

void load_lines_from_file_to_set(
    std::filesystem::path input_file_path,
    phmap::flat_hash_set<std::string_view> &lines_set
)
{
    std::ifstream input_file(input_file_path);
    std::string line;

    while (std::getline(input_file, line))
    {
        char *tmp_line = new char[line.size()];
        std::copy(line.begin(), line.end(), tmp_line);
        std::string_view line_sv(tmp_line, line.size());
        const auto &[it, inserted] = lines_set.emplace(line_sv);

        std::cout.flush();

        if (inserted == false)
        {
            delete[] tmp_line;
        }
    }

    input_file.close();
}

void load_lines_from_file_to_set2(
    std::vector<char>& data,
    phmap::flat_hash_set<std::string_view> &lines_set
) {

    char* buffer_end = &data[data.size()];

    char *start = data.data();
    char *end;

    while(start < buffer_end) {
        end = strchr(start, '\n');

        lines_set.emplace(std::string_view(start, end - start));

        start = end + 1;
    };
}

std::vector<char>* read_file(std::filesystem::path path) {

    auto file_size = std::filesystem::file_size(path);

    std::vector<char>* buffer = new std::vector<char>(file_size);

    int fd = open(path.c_str(), O_RDONLY);

    int index = 0;

    int num_read_bytes;

    while ( (num_read_bytes = read(fd, buffer->data() + index, 128 * 1024)) > 0) {
        index += num_read_bytes;
    }

    if (num_read_bytes == -1) {
        close(fd);
        throw std::exception();
    }

    close(fd);

    return buffer;
}

///////////////////

void test_load_lines_from_file_to_set() {
    std::vector<char> *buffer = read_file("data/small_new");

    phmap::flat_hash_set<std::string_view> lines_set;
    load_lines_from_file_to_set2(*buffer, lines_set);

    for (auto& o : lines_set) {
        std::cout << o << std::endl;
    }
}

void test_read_file() {
    std::vector<char>* buffer = read_file("data/small_new");

    std::cout << buffer->data();
}

// int main() {
//     test_load_lines_from_file_to_set();
//     // test_read_file();
// }
