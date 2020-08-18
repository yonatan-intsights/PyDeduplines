#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <fstream>
#include <string>
#include <string_view>
#include <vector>
#include <iostream>
#include <filesystem>
#include <thread>

#include <taskflow/taskflow.hpp>

#include <split_file.hpp>
#include <compare_files.hpp>
#include <pydeduplines.hpp>


std::filesystem::path make_part_path(
    std::filesystem::path output_directory,
    std::string prefix,
    int index
) {
    char filename[32];
    snprintf(filename, 32, "%s%05d", prefix.c_str(), index);

    std::filesystem::path part_path = output_directory / filename;

    return part_path;
}

void compute_files_added_lines(
    std::string _old_file_path,
    std::string _new_file_path,
    std::string _output_file_path,
    int max_mem_mega_bytes,
    int num_threads_suggestion)
{
    std::filesystem::path old_file_path(_old_file_path);
    std::filesystem::path new_file_path(_new_file_path);
    std::filesystem::path output_file_path(_output_file_path);

    char dir_tepmlate[] = "./dedup_XXXXXX";
    std::string tmpdir = mkdtemp(dir_tepmlate);

    int num_threads = get_num_threads(num_threads_suggestion);

    int num_parts = compute_num_parts(
        num_threads,
        old_file_path,
        new_file_path,
        max_mem_mega_bytes);

    split_files(
        old_file_path,
        new_file_path,
        tmpdir,
        num_parts
    );

    compute_parts_added_lines(
        num_threads,
        tmpdir,
        num_parts,
        output_file_path
    );

    std::filesystem::remove_all(std::filesystem::path(tmpdir));

    return;
}

void compute_parts_added_lines(
    int num_threads,
    std::string work_directory,
    int num_parts,
    std::filesystem::path output_file_path
) {
    tf::Taskflow taskflow;

    FILE* output_file = fopen(output_file_path.c_str(), "w");
    if (output_file == NULL) {
    }

    for (int i = 0; i < num_parts; i++) {
        std::filesystem::path old_file_part_path = make_part_path(
            work_directory,
            "old_",
            i
        );

        std::filesystem::path new_file_part_path = make_part_path(
            work_directory,
            "new_",
            i);

        taskflow.emplace(
            [old_file_part_path, new_file_part_path, output_file] {
                compute_added_lines(old_file_part_path, new_file_part_path, output_file);
            });
    }

    tf::Executor executor(num_threads);
    executor.run(taskflow).wait();

    fclose(output_file);

    return;
}

int get_num_threads(
    int num_threads_suggestion
) {
    if (num_threads_suggestion != -1)
        return num_threads_suggestion;

    const auto processor_count = std::thread::hardware_concurrency();

    return processor_count;
}

int compute_num_parts(
    int num_threads,
    std::filesystem::path old_file_path,
    std::filesystem::path new_file_path,
    int max_mem_mega_bytes
) {
    auto file_size1 = std::filesystem::file_size(old_file_path);
    auto file_size2 = std::filesystem::file_size(new_file_path);

    int num_parts = num_threads * 2 * (std::max(file_size1, file_size2) / (max_mem_mega_bytes));

    return num_parts;
}

PYBIND11_MODULE(pydeduplines, m)
{
    m.def(
        "compute_files_added_lines",
        &compute_files_added_lines,
        "Prints new lines that are in the new file and no in the original file.",
        pybind11::arg("original_file_path"),
        pybind11::arg("changed_file_path"),
        pybind11::arg("output_file_path"),
        pybind11::arg("memory_usage"),
        pybind11::arg("num_threads"));
}
