#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <fstream>
#include <string>
#include <vector>
#include <filesystem>

#include "mimalloc/static.c"
#include "mimalloc/include/mimalloc-override.h"
#include "mimalloc/include/mimalloc-new-delete.h"
#include "parallel_hashmap/phmap.h"

#define MAX_LINE_SIZE 1024

std::vector<char>* read_file(std::filesystem::path path)
{

    auto file_size = std::filesystem::file_size(path);

    std::vector<char> *buffer = new std::vector<char>(file_size);

    int fd = open(path.c_str(), O_RDONLY);

    int index = 0;

    int num_read_bytes;

    while ((num_read_bytes = read(fd, buffer->data() + index, 128 * 1024)) > 0)
    {
        index += num_read_bytes;
    }

    if (num_read_bytes == -1)
    {
        close(fd);
        throw std::exception();
    }

    close(fd);

    return buffer;
}

void load_lines_from_file_to_set(
    std::vector<char> &data,
    phmap::flat_hash_set<std::string_view> &lines_set)
{

    char *buffer_end = &data[data.size()];

    char *start = data.data();
    char *end;

    while (start < buffer_end)
    {
        end = strchr(start, '\n');

        lines_set.emplace(std::string_view(start, end - start));

        start = end + 1;
    };
}

void print_file_lines_not_in_set(
    std::filesystem::path file_path,
    phmap::flat_hash_set<std::string_view> &lines_set)
{
    FILE *change_file = fopen(file_path.c_str(), "r");
    if (!change_file)
    {
        std::cout << "file not opened: " << file_path << std::endl;
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

            std::cout << line_buf << "\n" ;
        }
    }
}

void compute_added_lines(
    std::string _original_file_path,
    std::string _new_file_path
)
{
    std::filesystem::path original_file_path(_original_file_path);
    std::filesystem::path new_file_path(_new_file_path);

    std::vector<char>* original_file_data = read_file(original_file_path);

    int num_lines = std::count(original_file_data->begin(), original_file_data->end(), '\n');

    std::cout << "num lines" << std::endl;

    phmap::flat_hash_set<std::string_view> lines_set(num_lines);
}

PYBIND11_MODULE(pydeduplines, m) {
    m.def(
        "compute_added_lines",
        &compute_added_lines,
        "Prints new lines that are in the new file and no in the original file.",
        pybind11::arg("original_file_path"),
        pybind11::arg("new_file_path"));
}
