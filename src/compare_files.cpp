#include <execution>
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

void read_file(std::filesystem::path path, std::vector<char>& buffer);

void load_lines_from_file_to_set(
    std::vector<char>& data,
    phmap::flat_hash_set<std::string_view> &lines_set
);

void check_file_lines_not_in_set(
    std::filesystem::path changed_file_path,
    phmap::flat_hash_set<std::string_view> &lines_set,
    std::vector<std::string>& result
);

void compute_added_lines(
    std::filesystem::path original_file_path,
    std::filesystem::path changed_file_path,
    std::vector<std::string>& result
) {
    std::vector<char> original_file_data;

    read_file(original_file_path, original_file_data);

    auto file_size = std::filesystem::file_size(original_file_path);

    int num_lines = std::count(std::execution::par, original_file_data.begin(), original_file_data.end(), '\n');

    phmap::flat_hash_set<std::string_view> lines_set(num_lines);

    load_lines_from_file_to_set(original_file_data, lines_set);

    check_file_lines_not_in_set(
        changed_file_path,
        lines_set,
        result
    );

}

void check_file_lines_not_in_set(
    std::filesystem::path changed_file_path,
    phmap::flat_hash_set<std::string_view> &lines_set,
    std::vector<std::string>& result
)
{
    FILE *change_file = fopen(changed_file_path.c_str(), "r");
    if (!change_file)
    {
        // std::cout << "file not opened: " << changed_file_path << std::endl;
        throw std::runtime_error("fail to open file");
    }

    int num_new_found = 0;

    char line_buf[MAX_LINE_SIZE] = {};

    size_t len_line;
    while (fgets(line_buf, MAX_LINE_SIZE, change_file))
    {
        len_line = strlen(line_buf) - 1;
        // std::cout << "len line:" << len_line << std::endl;

        std::string_view line_sv(line_buf, len_line);

        bool contained = lines_set.contains(line_sv);
        if (!contained) {
            num_new_found += 1;
            result.push_back(std::string(line_sv));
        }
    }

    fclose(change_file);
}

void load_lines_from_file_to_set(
    std::vector<char>& data,
    phmap::flat_hash_set<std::string_view> &lines_set
) {
    if (data.size() == 0) {
        return;
    }

    char* buffer_end = data.data() + data.size();

    char *start = data.data();
    char *end;

    while(start < buffer_end) {
        end = strchr(start, '\n');
        lines_set.emplace(std::string_view(start, end - start));

        start = end + 1;
    };
}

void read_file(std::filesystem::path path, std::vector<char>& buffer)
{
    auto file_size = std::filesystem::file_size(path);

    buffer.resize(file_size);

    int fd = open(path.c_str(), O_RDONLY);

    int index = 0;

    int num_read_bytes;

    int num_bytes_left = file_size;
    while ( (num_read_bytes = read(fd, buffer.data() + index, std::min(128 * 1024, num_bytes_left))) > 0) {
        index += num_read_bytes;
        num_bytes_left -= num_read_bytes;
    }

    if (num_read_bytes == -1) {
        close(fd);
        throw std::exception();
    }

    close(fd);

    return;
}
