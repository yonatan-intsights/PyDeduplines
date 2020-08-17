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

            result = pydeduplines.compute_files_added_lines(
                original_file_path=file1.name,
                new_file_path=file2.name,
                memory_usage=3,
                num_threads=-1,
            )

            expected = [
                'line1',
                'line3',
                'line5',
                'line7',
            ]
            self.assertEqual(
                first=expected,
                second=result,
            )
