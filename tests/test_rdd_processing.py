import unittest

import sys
from pathlib import Path

sys.path.append(
    str(Path(__file__).resolve().parents[1] / "src")
)

from rdd_processing import parse_log_line


class ParseLogLineTests(unittest.TestCase):
    def test_valid_combined_log_line(self):
        line = (
            '192.168.10.24 - - [10/Oct/2000:13:55:36 -0700] '
            '"GET /products?id=7 HTTP/1.1" 200 1234 '
            '"https://example.com/" "Mozilla/5.0"'
        )

        self.assertEqual(
            parse_log_line(line),
            {
                "ip": "192.168.10.24",
                "timestamp": "10/Oct/2000:13:55:36 -0700",
                "method": "GET",
                "endpoint": "/products?id=7",
                "status_code": 200,
            },
        )

    def test_missing_field_is_invalid(self):
        line = '192.168.10.24 - - [10/Oct/2000:13:55:36 -0700] "GET / HTTP/1.1"'
        self.assertIsNone(parse_log_line(line))

    def test_invalid_ip_is_rejected(self):
        line = (
            '999.168.10.24 - - [10/Oct/2000:13:55:36 -0700] '
            '"GET / HTTP/1.1" 200 12'
        )
        self.assertIsNone(parse_log_line(line))

    def test_non_numeric_status_is_rejected(self):
        line = (
            '192.168.10.24 - - [10/Oct/2000:13:55:36 -0700] '
            '"GET / HTTP/1.1" OK 12'
        )
        self.assertIsNone(parse_log_line(line))

    def test_malformed_request_is_rejected(self):
        line = (
            '192.168.10.24 - - [10/Oct/2000:13:55:36 -0700] '
            '"GET-only" 200 12'
        )
        self.assertIsNone(parse_log_line(line))


if __name__ == "__main__":
    unittest.main()
