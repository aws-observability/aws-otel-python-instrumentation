# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from pylint.checkers import BaseRawFileChecker

COPYWRITE_STRING = "# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.\n"
COPYWRITE_BYTES = bytes(COPYWRITE_STRING, "utf-8")
LICENSE_STRING = "# SPDX-License-Identifier: Apache-2.0"
LICENSE_BYTES = bytes(LICENSE_STRING, "utf-8")


class FileHeaderChecker(BaseRawFileChecker):
    name = "file_header_checker"
    msgs = {
        "E1234": (
            "File has missing or malformed header",
            "missing-header",
            "All files must have required header: \n" + COPYWRITE_STRING + LICENSE_STRING,
        ),
    }
    options = ()

    def process_module(self, node):
        """
        Check if the file has the required header in first and second lines of
        the file. Some files may be scripts, which requires the first line to
        be the shebang line, so handle that by ignoring the first line.
        """
        first_line = 0
        second_line = 1
        with node.stream() as stream:
            for line_num, line in enumerate(stream):
                if line_num == first_line and line.startswith(b"#!"):
                    first_line += 1
                    second_line += 1
                elif line_num == first_line and is_bad_copywrite_line(line):
                    self.add_message("missing-header", line=line_num)
                    break
                elif line_num == second_line and is_bad_license_line(line):
                    self.add_message("missing-header", line=line_num)
                    break
                elif line_num > second_line:
                    break


def is_bad_copywrite_line(line: bytes) -> bool:
    return not line.startswith(COPYWRITE_BYTES)


def is_bad_license_line(line: bytes) -> bool:
    return not line.startswith(LICENSE_BYTES)


def register(linter) -> None:
    linter.register_checker(FileHeaderChecker(linter))
