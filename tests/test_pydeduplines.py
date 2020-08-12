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
            test_input_file_one = stack.enter_context(tempfile.NamedTemporaryFile('w'))
            test_input_file_two = stack.enter_context(tempfile.NamedTemporaryFile('w'))


            test_input_file_one.file.write(
                'line1\n'
                'line2\n'
                'line3\n'
            )
            test_input_file_one.file.flush()
            test_input_file_two.file.write(
                'line1\n'
                'line2\n'
                'line3\n'
                'line5\n'
                'line1\n'
                'line3\n'
                'line4\n'
                'line1\n'
            )
            test_input_file_two.file.flush()

            pydeduplines.compute_added_lines(
                original_file_path=test_input_file_one.name,
                new_file_path=test_input_file_two.name,
            )
