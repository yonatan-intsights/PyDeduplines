import tempfile
import unittest
import unittest.mock
import contextlib

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

            lines = output_file.readlines()
            print(lines)

            expected = [
                'line1\n',
                'line3\n',
                'line5\n',
                'line7\n',
            ]
            self.assertEqual(
                first=expected,
                second=lines,
            )
