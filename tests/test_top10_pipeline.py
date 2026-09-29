"""Task 1.4(b): exact-match check of the Top 10 Countries pipeline.

Rather than re-deriving the expected answer from the generator, this test runs
the pipeline over a small hand-written log whose correct answer was worked out
by hand and written down below. It is an independent oracle: any change to the
parser (ID1) or to the Broadcast enrichment (ID2) that alters the result will
fail here.

The log lines are held in this file rather than in a separate data file, so the
records and the hand-computed answer can be reviewed side by side. They are
written to a temporary directory at run time, so ``build_parsed_rdd`` still
reads them through ``textFile`` exactly as it does in production.

The fixture is built so that each of the following is actually exercised:

* Two prefixes fold into one country, twice (Vietnam and the United States), so
  a pipeline that keyed on the prefix instead of the country would be caught.
* Eleven countries appear but only ten may be reported, so both the truncation
  and the alphabetical tie-break are exercised at the cut-off.
* All four malformed lines carry the Vietnam prefix, so a parser that let any
  of them through would push Vietnam above its expected count of three.
* One valid record uses a prefix absent from the lookup table, so the pipeline
  must drop it rather than fail or count it.
"""

import os
import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"

sys.path.insert(0, str(SRC_DIR))

# A PySpark 3.5 Python worker is a separate OS process and does not inherit the
# driver's in-memory sys.path, so the line above is not enough for a worker to
# import rdd_processing out of src/. The environment is inherited, and it has to
# be set before the first worker starts. See REPORT.md, section 3.2.6.
os.environ["PYTHONPATH"] = os.pathsep.join(
    entry for entry in (str(SRC_DIR), os.environ.get("PYTHONPATH", "")) if entry
)

from generate_logs import IP_COUNTRY_MAP
from rdd_processing import (
    aggregate_country_access,
    build_country_broadcast,
    build_parsed_rdd,
    enrich_with_country,
    parse_log_line,
    to_top10_dataframe,
)

try:
    from pyspark.sql import SparkSession

    PYSPARK_AVAILABLE = True
except ImportError:  # pragma: no cover - depends on the local environment
    PYSPARK_AVAILABLE = False


# --------------------------------------------------------------------------
# The hand-written log. Fifteen records resolve to a country, one valid record
# uses an unknown prefix, and four lines are malformed, one per reject reason.
# --------------------------------------------------------------------------

FIXTURE_LINES = [
    # Vietnam, from two different prefixes.
    '10.10.1.7 - - [01/Sep/2026:08:00:01 +0700] "GET /index.html HTTP/1.1" 200 1024 "https://example.com/" "Mozilla/5.0"',
    '10.10.1.8 - - [01/Sep/2026:08:00:02 +0700] "POST /api/v1/login HTTP/1.1" 201 512 "https://example.com/" "curl/8.4.0"',
    '10.10.2.9 - - [01/Sep/2026:08:00:03 +0700] "GET /products?id=1 HTTP/1.1" 200 2048 "https://example.com/" "Mozilla/5.0"',
    # Japan, tying with the United States on two accesses each.
    '10.20.1.10 - - [01/Sep/2026:08:00:04 +0700] "GET /static/css/main.css HTTP/2.0" 200 4096 "https://example.com/" "Mozilla/5.0"',
    '10.20.1.11 - - [01/Sep/2026:08:00:05 +0700] "GET /api/v1/orders HTTP/1.1" 404 128 "https://example.com/" "curl/8.4.0"',
    # United States, also from two different prefixes.
    '10.40.1.12 - - [01/Sep/2026:08:00:06 +0700] "PUT /api/v1/cart HTTP/1.1" 200 256 "https://example.com/" "Mozilla/5.0"',
    '10.40.2.13 - - [01/Sep/2026:08:00:07 +0700] "DELETE /api/v1/cart/9 HTTP/1.1" 204 0 "https://example.com/" "curl/8.4.0"',
    # Eight countries with a single access each; the last of them is cut.
    '10.30.1.14 - - [01/Sep/2026:08:00:08 +0700] "GET /search?q=spark HTTP/1.1" 200 3072 "https://search.example.net/?q=spark" "Mozilla/5.0"',
    '10.50.1.15 - - [01/Sep/2026:08:00:09 +0700] "GET /index.html HTTP/1.0" 304 0 "https://example.com/" "Mozilla/5.0"',
    '10.60.1.16 - - [01/Sep/2026:08:00:10 +0700] "POST /api/v1/checkout HTTP/1.1" 500 640 "https://example.com/" "curl/8.4.0"',
    '10.70.1.17 - - [01/Sep/2026:08:00:11 +0700] "GET /static/js/app.js HTTP/2.0" 200 8192 "https://example.com/" "Mozilla/5.0"',
    '10.80.1.18 - - [01/Sep/2026:08:00:12 +0700] "GET /about HTTP/1.1" 200 1536 "https://example.com/" "Mozilla/5.0"',
    '10.90.1.19 - - [01/Sep/2026:08:00:13 +0700] "HEAD /health HTTP/1.1" 200 0 "https://example.com/" "curl/8.4.0"',
    '192.168.10.20 - - [01/Sep/2026:08:00:14 +0700] "GET /products?id=7 HTTP/1.1" 200 2560 "https://example.com/" "Mozilla/5.0"',
    '192.168.20.21 - - [01/Sep/2026:08:00:15 +0700] "GET /contact HTTP/1.1" 200 900 "https://example.com/" "Mozilla/5.0"',
    # Valid, but 172.16.99 is not in IP_COUNTRY_MAP, so it must be dropped.
    '172.16.99.22 - - [01/Sep/2026:08:00:16 +0700] "GET /index.html HTTP/1.1" 200 1024 "https://example.com/" "Mozilla/5.0"',
    # Four malformed lines, all on the Vietnam prefix so that a parser leak
    # would move Vietnam off its expected count of three.
    '10.10.1.23 - - [01/Sep/2026:08:00:17 +0700] "GET /index.html HTTP/1.1"',  # missing status and bytes
    '10.10.1.999 - - [01/Sep/2026:08:00:18 +0700] "GET /index.html HTTP/1.1" 200 1024 "https://example.com/" "Mozilla/5.0"',  # octet out of range
    '10.10.1.25 - - [01/Sep/2026:08:00:19 +0700] "GET /index.html HTTP/1.1" NaN 1024 "https://example.com/" "Mozilla/5.0"',  # non-numeric status
    '10.10.1.26 - - [01/Sep/2026:08:00:20 +0700] "GET-only" 200 1024 "https://example.com/" "Mozilla/5.0"',  # malformed request
]


# --------------------------------------------------------------------------
# The expected answer, computed by hand from the lines above.
# --------------------------------------------------------------------------

TOTAL_LINES = 20
EXPECTED_VALID = 16
EXPECTED_MALFORMED = 4
EXPECTED_WITH_COUNTRY = 15  # the sixteenth valid record has an unknown prefix

EXPECTED_ALL_COUNTS = {
    "Vietnam": 3,
    "Japan": 2,
    "United States": 2,
    "Australia": 1,
    "Brazil": 1,
    "France": 1,
    "Germany": 1,
    "India": 1,
    "Singapore": 1,
    "South Korea": 1,
    "United Kingdom": 1,
}

# to_top10_dataframe orders on (-count, country), so equal counts fall back to
# alphabetical order. United Kingdom is the eleventh country and is cut.
EXPECTED_TOP10 = [
    ("Vietnam", 3),
    ("Japan", 2),
    ("United States", 2),
    ("Australia", 1),
    ("Brazil", 1),
    ("France", 1),
    ("Germany", 1),
    ("India", 1),
    ("Singapore", 1),
    ("South Korea", 1),
]

EXCLUDED_COUNTRY = "United Kingdom"


class FixtureIntegrityTests(unittest.TestCase):
    """Guard the fixture and the hand-written answer themselves."""

    def test_fixture_line_count_is_unchanged(self):
        self.assertEqual(len(FIXTURE_LINES), TOTAL_LINES)

    def test_hand_written_answer_is_self_consistent(self):
        self.assertEqual(EXPECTED_VALID + EXPECTED_MALFORMED, TOTAL_LINES)
        self.assertEqual(sum(EXPECTED_ALL_COUNTS.values()), EXPECTED_WITH_COUNTRY)
        self.assertEqual(len(EXPECTED_TOP10), 10)
        self.assertEqual(len(EXPECTED_ALL_COUNTS), len(EXPECTED_TOP10) + 1)
        self.assertNotIn(EXCLUDED_COUNTRY, dict(EXPECTED_TOP10))


class FixtureOracleTests(unittest.TestCase):
    """Re-derive the answer in plain Python, with no Spark involved.

    This keeps the fixture checkable on a machine with no Java or PySpark, and
    cross-checks the Spark result below against a second implementation.
    """

    @classmethod
    def setUpClass(cls):
        cls.records = []
        cls.malformed = 0
        for line in FIXTURE_LINES:
            record = parse_log_line(line)
            if record is None:
                cls.malformed += 1
            else:
                cls.records.append(record)

    def test_valid_and_malformed_split(self):
        self.assertEqual(len(self.records), EXPECTED_VALID)
        self.assertEqual(self.malformed, EXPECTED_MALFORMED)

    def test_country_totals_match_the_hand_written_answer(self):
        tally = Counter()
        for record in self.records:
            country = IP_COUNTRY_MAP.get(record["ip"].rsplit(".", 1)[0])
            if country:
                tally[country] += 1
        self.assertEqual(dict(tally), EXPECTED_ALL_COUNTS)


@unittest.skipUnless(PYSPARK_AVAILABLE, "pyspark is not installed")
class Top10PipelineTests(unittest.TestCase):
    """Run the real pipeline end to end over the fixture."""

    @classmethod
    def setUpClass(cls):
        cls.tmp_dir = tempfile.TemporaryDirectory(prefix="id2-top10-")
        log_file = Path(cls.tmp_dir.name) / "top10_fixture.log"
        log_file.write_text("\n".join(FIXTURE_LINES) + "\n", encoding="utf-8")

        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("id2-task-1.4-top10-fixture")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        spark_context = cls.spark.sparkContext
        spark_context.setLogLevel("ERROR")

        cls.invalid_log_counter = spark_context.accumulator(0)
        country_broadcast = build_country_broadcast(spark_context, IP_COUNTRY_MAP)

        # cache_result=True materializes the RDD once, so the later actions read
        # the cache and cannot increment the accumulator a second time.
        parsed_rdd = build_parsed_rdd(
            spark_context,
            log_file.as_uri(),
            invalid_log_counter=cls.invalid_log_counter,
            cache_result=True,
        )
        cls.valid_count = parsed_rdd.count()

        enriched_rdd = enrich_with_country(parsed_rdd, country_broadcast)
        cls.with_country_count = enriched_rdd.filter(
            lambda record: bool(record.get("country"))
        ).count()

        counts_rdd = aggregate_country_access(enriched_rdd)
        cls.all_counts = dict(counts_rdd.collect())
        cls.top_ten = [
            (row["country"], row["access_count"])
            for row in to_top10_dataframe(counts_rdd, cls.spark).collect()
        ]

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.tmp_dir.cleanup()

    def test_valid_and_malformed_split(self):
        self.assertEqual(self.valid_count, EXPECTED_VALID)
        self.assertEqual(self.invalid_log_counter.value, EXPECTED_MALFORMED)

    def test_invariant_valid_plus_malformed_equals_total_lines(self):
        self.assertEqual(
            self.valid_count + self.invalid_log_counter.value, TOTAL_LINES
        )

    def test_record_with_an_unknown_prefix_is_dropped_not_counted(self):
        self.assertEqual(self.with_country_count, EXPECTED_WITH_COUNTRY)
        self.assertEqual(sum(self.all_counts.values()), EXPECTED_WITH_COUNTRY)

    def test_every_country_count_matches_the_hand_written_answer(self):
        self.assertEqual(self.all_counts, EXPECTED_ALL_COUNTS)

    def test_top_ten_matches_exactly_including_order(self):
        self.assertEqual(self.top_ten, EXPECTED_TOP10)

    def test_eleventh_country_is_present_but_excluded_from_the_top_ten(self):
        self.assertEqual(self.all_counts[EXCLUDED_COUNTRY], 1)
        self.assertNotIn(EXCLUDED_COUNTRY, [country for country, _ in self.top_ten])


if __name__ == "__main__":
    unittest.main()
