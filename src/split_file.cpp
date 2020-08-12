#include <fstream>
#include <string>
#include <string_view>
#include <vector>
#include <iostream>
#include <filesystem>
#include <thread>

#include <boost/asio/thread_pool.hpp>
#include <boost/asio/post.hpp>


void split_file(
    std::filesystem::path input_file_path,
    std::filesystem::path output_directory,
    std::vector<FILE *> &output_files
);
void close_files(
    std::vector<FILE *> files
);
std::vector<FILE *> create_split_output_files(
    int num_files,
    std::filesystem::path output_directory,
    std::string file_prefix
);
std::filesystem::path make_part_path(
    std::filesystem::path output_directory,
    std::string prefix,
    int index
);


void split_files(
    std::filesystem::path old_file_path,
    std::filesystem::path new_file_path,
    std::filesystem::path output_directory,
    int num_parts
) {
    boost::asio::thread_pool split_pool(2);

    std::cout << "started splitting" << std::endl;

    boost::asio::post(split_pool, [old_file_path, output_directory, num_parts] {
        char file_prefix[] = "old_";

        std::vector<FILE *> output_files = create_split_output_files(
            num_parts,
            output_directory,
            file_prefix);

        split_file(old_file_path, output_directory, output_files);

        close_files(output_files);
    });

    boost::asio::post(split_pool, [new_file_path, output_directory, num_parts] {
        char file_prefix[] = "new_";

        std::vector<FILE *> output_files = create_split_output_files(
            num_parts,
            output_directory,
            file_prefix);

        split_file(new_file_path, output_directory, output_files);

        close_files(output_files);
    });

    split_pool.join();

    std::cout << "ended splitting" << std::endl;
}

void split_file(
    std::filesystem::path input_file_path,
    std::filesystem::path output_directory,
    std::vector<FILE *> &output_files
)
{
    char line_buf[4012] = {};
    int num_files = output_files.size();

    FILE *fp = fopen(input_file_path.c_str(), "r");

    char c;
    char *str;
    while (fgets(line_buf, 4012, fp))
    {
        str = line_buf;
        unsigned long hash = 5381;

        // printf("line: %s", str);
        c = *str;
        while (c && c != '\n')
        {
            //printf("char: %c %d\n", c, int(c));
            hash = ((hash << 5) + hash) + c;
            str++;
            c = *str;
        }

        // printf("hash %d\n", hash);
        // printf("index %d\n", index);
        unsigned int index = (unsigned int)hash % num_files;
        fputs(line_buf, output_files[index]);
    }

    fclose(fp);
}

void close_files(
    std::vector<FILE *> files
)
{
    for (int i = 0; i < files.size(); i++)
    {
        fclose(files[i]);
        files[i] = nullptr;
    }
}

std::vector<FILE *> create_split_output_files(
    int num_files,
    std::filesystem::path output_directory,
    std::string file_prefix
)
{
    std::vector<FILE *> output_files(num_files);

    for (int i = 0; i < num_files; i++) {
        std::filesystem::path output_file_path = make_part_path(
            output_directory,
            file_prefix,
            i);

        output_files[i] = fopen(output_file_path.c_str(), "w");
    }

    return output_files;
}
