"""Core RDD pipeline for parsing and aggregating web access logs.

The parser is deliberately independent from PySpark so it can be unit-tested
without starting a Spark session.
"""

from __future__ import annotations

import ipaddress
import re
from operator import add
from typing import Any, Dict, Iterable, Optional


# Provisional Common/Combined Log Format pattern. Keep this in one place so it
# can be adjusted when the final generator output from ID2 is available.
LOG_PATTERN = re.compile(
    r'^(?P<ip>\S+)\s+\S+\s+\S+\s+'
    r'\[(?P<timestamp>[^\]]+)\]\s+'
    r'"(?P<request>[^"]*)"\s+'
    r'(?P<status>\S+)\s+'
    r'(?P<bytes>\S+)'
    r'(?:\s+"[^"]*"\s+"[^"]*")?\s*$'
)

REQUEST_PATTERN = re.compile(
    r"^(?P<method>[A-Z]+)\s+(?P<endpoint>\S+)\s+HTTP/\d+(?:\.\d+)?$"
)


def parse_log_line(line: str) -> Optional[Dict[str, Any]]:
    """Parse one Common/Combined Log Format line.

    Return ``None`` for malformed input so bad records can be filtered without
    terminating a Spark job.
    """

    if not isinstance(line, str):
        return None

    match = LOG_PATTERN.fullmatch(line.strip())
    if match is None:
        return None

    try:
        ip = str(ipaddress.IPv4Address(match.group("ip")))
        status_code = int(match.group("status"))
    except (ipaddress.AddressValueError, ValueError):
        return None

    if not 100 <= status_code <= 599:
        return None

    request_match = REQUEST_PATTERN.fullmatch(match.group("request"))
    if request_match is None:
        return None

    return {
        "ip": ip,
        "timestamp": match.group("timestamp"),
        "method": request_match.group("method"),
        "endpoint": request_match.group("endpoint"),
        "status_code": status_code,
    }


def _expand_text_record(raw_record: str) -> Iterable[str]:
    """Expand a text record into logical lines for the required flatMap step."""

    lines = raw_record.splitlines()
    return lines if lines else (raw_record,)


def _parse_with_counter(line: str, invalid_log_counter: Any = None):
    record = parse_log_line(line)
    if record is None and invalid_log_counter is not None:
        invalid_log_counter.add(1)
    return record


def build_parsed_rdd(
    spark_context: Any,
    input_path: str,
    invalid_log_counter: Any = None,
    cache_result: bool = False,
):
    """Build an RDD containing only valid parsed records.

    ``invalid_log_counter`` is an optional integration seam for ID2. If a
    counter is supplied and the result feeds multiple actions, set
    ``cache_result=True``: the function caches and materializes the valid RDD
    once, avoiding repeated evaluation of the counter update in normal use.
    Spark accumulators are not an exactly-once counting mechanism.
    """

    raw_rdd = spark_context.textFile(input_path)
    logical_lines_rdd = raw_rdd.flatMap(_expand_text_record)
    parsed_or_none_rdd = logical_lines_rdd.map(
        lambda line: _parse_with_counter(line, invalid_log_counter)
    )
    parsed_rdd = parsed_or_none_rdd.filter(lambda record: record is not None)

    if cache_result:
        parsed_rdd.cache()
        parsed_rdd.count()

    return parsed_rdd


def aggregate_country_access(enriched_rdd: Any):
    """Count accesses by country with a Pair RDD and ``reduceByKey``.

    ``enriched_rdd`` is the hand-off point from ID2's Broadcast enrichment and
    must contain parsed dictionaries with a non-empty ``country`` field.
    """

    records_with_country = enriched_rdd.filter(
        lambda record: bool(record.get("country"))
    )
    country_pairs = records_with_country.map(lambda record: (record["country"], 1))
    return country_pairs.reduceByKey(add)


def to_top10_dataframe(country_counts_rdd: Any, spark: Any):
    """Select the top ten country counts, then perform the sole DF conversion."""

    top_ten = country_counts_rdd.takeOrdered(
        10, key=lambda item: (-item[1], item[0])
    )
    if not top_ten:
        return spark.createDataFrame([], "country string, access_count long")

    return spark.sparkContext.parallelize(top_ten).toDF(
        ["country", "access_count"]
    )
