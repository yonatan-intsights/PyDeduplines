// #include <pybind11/pybind11.h>
// #include <pybind11/stl.h>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>
#include <iostream>
#include <filesystem>
#include <thread>

#include <boost/asio/thread_pool.hpp>
#include <boost/asio/post.hpp>

void split_files(
    std::filesystem::path old_file_path,
    std::filesystem::path new_file_path,
    std::filesystem::path output_directory,
    int num_parts
);

int get_num_threads(
    int num_threads_suggestion
);

int compute_num_parts(
    int num_threads,
    std::filesystem::path old_file_path,
    std::filesystem::path new_file_path,
    int max_mem_mega_bytes
);
std::vector<std::string> compute_parts_added_lines(
    int num_threads,
    std::string work_directory,
    int num_parts
);

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

void compute_added_lines(
    std::filesystem::path original_file_path,
    std::filesystem::path changed_file_path,
    std::vector<std::string>& result
);

void compute_files_added_lines(
    std::filesystem::path old_file_path,
    std::filesystem::path new_file_path,
    int max_mem_mega_bytes,
    int max_threads
);

int main(int argc, char **argv)
{
    if (argc != 5) {
        std::cout << "usage: oldfile newfile MBs num_threads";
        return -1;
    }

    char* old_file_path = argv[1];
    char* new_file_path = argv[2];
    int max_mem_mbs = std::atoi(argv[3]);
    int num_threads = std::atoi(argv[4]);

    compute_files_added_lines(
        old_file_path,
        new_file_path,
        max_mem_mbs,
        num_threads
    );

    return 0;
}

void compute_files_added_lines(
    std::filesystem::path old_file_path,
    std::filesystem::path new_file_path,
    int max_mem_mega_bytes,
    int num_threads_suggestion
) {
    char dir_tepmlate[] = "./dedup_XXXXXX";
    std::string tmpdir = mkdtemp(dir_tepmlate);
    // std::string tmpdir = "./dedup_big_split";

    std::cout << "created tmp dir" << tmpdir << std::endl;

    int num_threads = get_num_threads(num_threads_suggestion);

    std::cout << "num threads chosen: " << num_threads << std::endl;

    int num_parts = compute_num_parts(
        num_threads,
        old_file_path,
        new_file_path,
        max_mem_mega_bytes);

    std::cout << "num parts: " << num_parts << std::endl;

    split_files(
        old_file_path,
        new_file_path,
        tmpdir,
        num_parts
    );

    std::vector<std::string> result = compute_parts_added_lines(
        num_threads,
        tmpdir,
        num_parts
    );

    // rmdir(tmpdir.c_str());
    // std::cout << "deleted tmp dir" << tmpdir << std::endl;
}

std::vector<std::string> compute_parts_added_lines(
    int num_threads,
    std::string work_directory,
    int num_parts
) {
    boost::asio::thread_pool diff_pool(num_threads);

    std::vector<std::vector<std::string>> parts_results(num_parts);

    std::cout << "started diffing" << std::endl;

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

        boost::asio::post(diff_pool, [old_file_part_path, new_file_part_path, &parts_results, i] {
            compute_added_lines(old_file_part_path, new_file_part_path, parts_results[i]);
        });
    }

    diff_pool.join();
    std::cout << "ended diffing" << std::endl;

    std::cout << "collecting result" << std::endl;

    int total_num_results = 0;
    for (int i = 0; i < num_parts; i++) {
        total_num_results += parts_results[i].size();
    }
    std::cout << "total num results: " << total_num_results << std::endl;

    std::vector<std::string> results(total_num_results);

    for (int i = parts_results.size() - 1 ; i >= 0; i--)
    {
        for (std::string& s: parts_results[i]) {
            results.push_back(s);
        }

        parts_results.pop_back();
    }
    std::cout << "finished collecting result:" << std::endl;

    int total_size = 0;
    for (auto &s: results) {
        total_size += s.size();
    }

    std::cout << "total strings size: " << total_size << std::endl;

    return results;
}

int get_num_threads(
    int num_threads_suggestion
) {
    if (num_threads_suggestion != -1)
        return num_threads_suggestion;

    const auto processor_count = std::thread::hardware_concurrency();

    std::cout << "number of available cores is: " << processor_count << std::endl;

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

    std::cout << "The size of " << old_file_path.u8string() << " is " << file_size1 << " bytes.\n";
    std::cout << "The size of " << new_file_path.u8string() << " is " << file_size2 << " bytes.\n";

    int num_parts = num_threads * 2 * (std::max(file_size1, file_size2) / (max_mem_mega_bytes));

    return num_parts;
}
