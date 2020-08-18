import tempfile
import unittest
import unittest.mock
import contextlib
import psutil

import pydeduplines


class PyDeduplinesTestCase(
    unittest.TestCase,
):
    def test_pydeduplines(
        self,
    ):
        with contextlib.ExitStack() as stack:
            file1 = stack.enter_context(tempfile.NamedTemporaryFile('w'))
            file2 = stack.enter_context(tempfile.NamedTemporaryFile('w'))
            output_file = stack.enter_context(tempfile.NamedTemporaryFile('r'))

            file1.file.write(
                'line2\n'
                'line4\n'
                'line6\n'
                'line8\n'
            )
            file1.file.flush()

            file2.file.write(
                'line1\n'
                'line2\n'
                'line3\n'
                'line4\n'
                'line5\n'
                'line6\n'
                'line7\n'
            )
            file2.file.flush()

            pydeduplines.compute_files_added_lines(
                original_file_path=file1.name,
                changed_file_path=file2.name,
                output_file_path=output_file.name,
                memory_usage=3,
                num_threads=-1,
            )

            lines = set(output_file.readlines())

            expected = set([
                'line1\n',
                'line3\n',
                'line5\n',
                'line7\n',
            ])
            self.assertEqual(
                first=expected,
                second=lines,
            )

    def test_count_file_lines(self):
        with contextlib.ExitStack() as stack:
            file = stack.enter_context(tempfile.NamedTemporaryFile('w'))

            file.file.write(
                'line1\n'
                'line2\n'
                'line3\n'
                'line4\n'
                'line5\n'
                'line6\n'
                'line7\n'
            )
            file.flush()

            result = pydeduplines.count_file_lines(file.name)
            self.assertEqual(
                result,
                7
            )

    def test_compute_num_parts(self):
        result = pydeduplines.compute_num_parts(
            num_threads=2,
            original_file_path='src/data/medium_old',
            modified_file_path='src/data/medium_new',
            max_memory_bytes=100 * 1024 * 1024,
        )
        print("result is: ", result)

    def test_compute_num_parts2(self):
        result = pydeduplines.compute_num_parts(
            num_threads=8,
            original_file_path='src/data/medium_old',
            modified_file_path='src/data/medium_new',
            max_memory_bytes=104857600,
        )
        print("result is: ", result)

    def test_count_file_lines_on_medium(self):
        pydeduplines.count_file_lines('src/data/medium_old')

    def _test_benchark_map_peak_memory(self):
        million = 1000000
        num_entries_tests = [i for i in range(million, 10 * million, million)]
        results = []
        for i, num_entries in enumerate(num_entries_tests):
            print(i)
            mem_usage = pydeduplines.get_hashset_memory_usage2(num_entries)
            results.append(mem_usage)

        ratios = [memory * 1024 / entries for (memory, entries) in zip(results, num_entries_tests)]

        print(min(ratios), max(ratios))

        with open('hash_mem_benchmark_results.txt', 'w') as f:
            f.write("entries,mem_usage_kb\n")
            for (memory, entries) in zip(results, num_entries_tests):
                f.write(f"{entries},{memory}\n")

    def test_medium(self):
        pydeduplines.compute_files_added_lines(
            original_file_path='src/data/medium_old',
            changed_file_path='src/data/medium_new',
            output_file_path='medium_diff',
            memory_usage=100 * 1024 * 1024,
            num_threads=1,
        )
