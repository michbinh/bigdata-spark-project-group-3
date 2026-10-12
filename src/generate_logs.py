"""Generate synthetic web access logs for the Spark pipeline (Task 2.2 - ID2).

================================================================================
LOG FORMAT (the shared contract with ID1's core parser in
``rdd_processing.py``). Each record occupies ONE line in Combined Log Format:

    <ip> <ident> <authuser> [<timestamp>] "<method> <endpoint> HTTP/<ver>" <status> <bytes> "<referer>" "<user_agent>"

Example of a complete valid line::

    192.168.10.24 - - [10/Oct/2000:13:55:36 +0700] "GET /products?id=7 HTTP/1.1" 200 1234 "https://example.com/" "Mozilla/5.0"

Example of a shortened valid line (Common Log Format - last two fields omitted)::

    192.168.10.24 - - [10/Oct/2000:13:55:36 +0700] "GET /products?id=7 HTTP/1.1" 200 1234

Field conventions:

===============  ============================================================
Field            Description
===============  ============================================================
``ip``           Dotted-quad IPv4 address. This is the country lookup key.
``ident``        Always ``-`` (identd is not used).
``authuser``     Always ``-`` (no HTTP authentication).
``timestamp``    Enclosed in ``[]`` using ``dd/Mon/yyyy:HH:MM:SS +0700``.
                 Month names use three English letters and are locale-independent.
``request``      Enclosed in ``""`` and contains exactly three space-separated
                 parts: an uppercase method (``[A-Z]+``), an endpoint without
                 spaces, and the version ``HTTP/<major>[.<minor>]``.
``status``       Integer HTTP status code in the range 100..599.
``bytes``        Integer response size in bytes.
``referer``      Optional and enclosed in ``""``.
``user_agent``   Optional and enclosed in ``""``. ``referer`` and
                 ``user_agent`` must either both be present or both be absent.
===============  ============================================================

Fields are separated by spaces. The ``timestamp``, ``request``, ``referer``,
and ``user_agent`` fields may contain internal spaces, so they are enclosed in
``[]`` or ``""`` to remain distinguishable.

================================================================================
GENERATION RATIO: exactly 90% valid records and 10% malformed records. Counts
are allocated DETERMINISTICALLY rather than randomly, so the ratio remains exact
regardless of the seed. The malformed 10% is split evenly among the four types below.

MALFORMED-RECORD TAXONOMY (canonical, ORDERED BY PRIORITY, and mutually exclusive).
Each malformed record is classified by checking the following sequence and
stopping at the FIRST matching category:

    1. ``missing_field``      - Too few fields are available for the schema.
    2. ``invalid_ipv4``       - All fields exist, but the IP is not valid IPv4.
    3. ``non_numeric_status`` - All fields and the IP are valid, but the status
                                cannot be parsed as an integer.
    4. ``malformed_request``  - Fields, IP, and status are valid, but the
                                method, endpoint, or HTTP version is malformed.

Malformed records are generated INTENTIONALLY and must not be cleaned. They are
inputs used to verify the malformed-record accumulator in Task 2.4.

================================================================================
USAGE - run from the repository root with no arguments and no dependency beyond
the standard library (the script does NOT use PySpark)::

    python3 src/generate_logs.py

The default output is ``data/raw_logs.txt``. The path is derived from this
script's location, so execution from any working directory produces the same
result. No absolute path is hard-coded.
"""

from __future__ import annotations

import argparse
import ipaddress
import random
import re
from pathlib import Path
from typing import Dict, List, Tuple


# --------------------------------------------------------------------------
# Configuration constants
# --------------------------------------------------------------------------

#: Default total number of generated records. It is divisible by 40 so the
#: 90/10 split and four malformed categories require no rounding.
DEFAULT_TOTAL_RECORDS = 10_000

#: Required ratio of valid records.
VALID_RATIO = 0.9

#: Default seed for reproducible output across ID7 clean-clone QA runs.
DEFAULT_SEED = 20260922

#: Repository root. This script is in ``src/``, so the root is its parent.
REPO_ROOT = Path(__file__).resolve().parent.parent

#: Default output path relative to the repository root. ID6 defines this
#: location in the README, and ``!data/raw_logs.txt`` explicitly preserves it
#: in ``.gitignore``.
DEFAULT_OUTPUT_RELPATH = Path("data") / "raw_logs.txt"


# --------------------------------------------------------------------------
# IP-to-country lookup table (shared interface with the Task 2.4 broadcast)
# --------------------------------------------------------------------------

#: Keys are /24 prefixes (the first three IPv4 octets); values are country names.
#:
#: Consumer-side lookup (Task 2.4 broadcast enrichment)::
#:
#:     prefix = record["ip"].rsplit(".", 1)[0]
#:     country = broadcast_map.value.get(prefix)
#:
#: /24 prefixes keep the broadcast table small while covering all 254 host
#: addresses in each range.
#:
#: All ranges below are in private RFC 1918 address space. The data is synthetic
#: and does not point to the real infrastructure of any organisation.
IP_COUNTRY_MAP: Dict[str, str] = {
    "10.10.1": "Vietnam",
    "10.10.2": "Vietnam",
    "10.20.1": "Japan",
    "10.30.1": "Singapore",
    "10.40.1": "United States",
    "10.40.2": "United States",
    "10.50.1": "Germany",
    "10.60.1": "India",
    "10.70.1": "Brazil",
    "10.80.1": "Australia",
    "10.90.1": "France",
    "192.168.10": "South Korea",
    "192.168.20": "United Kingdom",
    "192.168.30": "Canada",
    "192.168.40": "Thailand",
    "192.168.50": "Indonesia",
}

#: Traffic weights by prefix create a clearly non-uniform Top 10 Countries
#: distribution, making the ranking easier to verify.
PREFIX_WEIGHTS: Dict[str, int] = {
    "10.10.1": 90,
    "10.10.2": 70,
    "10.20.1": 85,
    "10.30.1": 60,
    "10.40.1": 80,
    "10.40.2": 55,
    "10.50.1": 50,
    "10.60.1": 45,
    "10.70.1": 35,
    "10.80.1": 30,
    "10.90.1": 25,
    "192.168.10": 40,
    "192.168.20": 20,
    "192.168.30": 15,
    "192.168.40": 12,
    "192.168.50": 8,
}


# --------------------------------------------------------------------------
# Value pools for generating valid records
# --------------------------------------------------------------------------

MONTH_ABBREVIATIONS: Tuple[str, ...] = (
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
)

HTTP_METHODS: Tuple[str, ...] = ("GET", "GET", "GET", "GET", "POST", "PUT", "DELETE", "HEAD")

ENDPOINTS: Tuple[str, ...] = (
    "/",
    "/index.html",
    "/products",
    "/products?id=7",
    "/products?id=142&sort=price",
    "/cart",
    "/checkout",
    "/api/v1/users",
    "/api/v1/orders",
    "/api/v1/orders?page=3",
    "/search?q=spark",
    "/static/css/main.css",
    "/static/js/app.js",
    "/images/banner.png",
    "/login",
    "/logout",
    "/account/profile",
    "/blog/rdd-vs-dataframe",
)

STATUS_CODES: Tuple[int, ...] = (
    200, 200, 200, 200, 200, 200,
    201, 204, 301, 302, 304,
    400, 401, 403, 404, 404,
    500, 502, 503,
)

HTTP_VERSIONS: Tuple[str, ...] = ("HTTP/1.1", "HTTP/1.1", "HTTP/1.1", "HTTP/1.0", "HTTP/2.0")

REFERERS: Tuple[str, ...] = (
    "-",
    "https://example.com/",
    "https://example.com/products",
    "https://search.example.net/?q=spark",
    "https://partner.example.org/landing",
)

USER_AGENTS: Tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) Mobile/15E148",
    "curl/8.4.0",
)


# --------------------------------------------------------------------------
# Patterns used by the self-check
# --------------------------------------------------------------------------

#: Log-line structure matching the contract in the module docstring. This
#: pattern is used only by the generator's self-check. The source of truth for
#: processing is ID1's ``LOG_PATTERN`` in ``rdd_processing.py``; both patterns
#: must be structurally equivalent.
LOG_LINE_PATTERN = re.compile(
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

#: Labels for the four malformed categories in taxonomy priority order.
MALFORMED_KINDS: Tuple[str, ...] = (
    "missing_field",
    "invalid_ipv4",
    "non_numeric_status",
    "malformed_request",
)


# --------------------------------------------------------------------------
# Classify records by the ordered, mutually exclusive taxonomy
# --------------------------------------------------------------------------

def classify_line(line: str) -> str:
    """Classify one log line into EXACTLY ONE taxonomy label.

    Return ``"valid"`` or one of the four labels in
    :data:`MALFORMED_KINDS`. Checks strictly follow priority order
    1 -> 2 -> 3 -> 4 and stop at the first match, keeping categories mutually
    exclusive.

    The ``"status_out_of_range"`` label is outside the taxonomy and appears
    only if the generator is defective; :func:`_self_check` asserts that it
    never occurs.
    """

    match = LOG_LINE_PATTERN.fullmatch(line.strip())

    # (1) Missing field: not enough fields can be extracted for the schema.
    if match is None:
        return "missing_field"

    # (2) Invalid IPv4 format.
    try:
        ipaddress.IPv4Address(match.group("ip"))
    except ipaddress.AddressValueError:
        return "invalid_ipv4"

    # (3) The status code is not an integer.
    try:
        status_code = int(match.group("status"))
    except ValueError:
        return "non_numeric_status"

    if not 100 <= status_code <= 599:
        return "status_out_of_range"

    # (4) Malformed method, endpoint, or HTTP version in the request.
    if REQUEST_PATTERN.fullmatch(match.group("request")) is None:
        return "malformed_request"

    return "valid"


# --------------------------------------------------------------------------
# Generate individual record components
# --------------------------------------------------------------------------

def _random_prefix(rng: random.Random) -> str:
    """Select a random /24 prefix according to the traffic weights."""

    prefixes = list(PREFIX_WEIGHTS)
    weights = [PREFIX_WEIGHTS[prefix] for prefix in prefixes]
    return rng.choices(prefixes, weights=weights, k=1)[0]


def _random_ip(rng: random.Random) -> str:
    """Generate a valid IPv4 address from a declared /24 range."""

    return "{}.{}".format(_random_prefix(rng), rng.randint(1, 254))


def _random_timestamp(rng: random.Random) -> str:
    """Generate a locale-independent ``dd/Mon/yyyy:HH:MM:SS +0700`` timestamp."""

    return "{day:02d}/{month}/{year}:{hour:02d}:{minute:02d}:{second:02d} +0700".format(
        day=rng.randint(1, 28),
        month=rng.choice(MONTH_ABBREVIATIONS),
        year=2026,
        hour=rng.randint(0, 23),
        minute=rng.randint(0, 59),
        second=rng.randint(0, 59),
    )


def _random_request(rng: random.Random) -> str:
    """Generate a valid request component: ``METHOD endpoint HTTP/x.y``."""

    return "{} {} {}".format(
        rng.choice(HTTP_METHODS),
        rng.choice(ENDPOINTS),
        rng.choice(HTTP_VERSIONS),
    )


def _format_line(
    ip: str,
    timestamp: str,
    request: str,
    status: str,
    num_bytes: str,
    referer: str = None,
    user_agent: str = None,
) -> str:
    """Combine the components into a complete log line."""

    line = '{ip} - - [{ts}] "{req}" {status} {nbytes}'.format(
        ip=ip, ts=timestamp, req=request, status=status, nbytes=num_bytes
    )
    if referer is not None and user_agent is not None:
        line += ' "{}" "{}"'.format(referer, user_agent)
    return line


def generate_valid_line(rng: random.Random) -> str:
    """Generate a complete VALID record that follows the format contract."""

    include_optional_fields = rng.random() < 0.75
    return _format_line(
        ip=_random_ip(rng),
        timestamp=_random_timestamp(rng),
        request=_random_request(rng),
        status=str(rng.choice(STATUS_CODES)),
        num_bytes=str(rng.randint(0, 98_304)),
        referer=rng.choice(REFERERS) if include_optional_fields else None,
        user_agent=rng.choice(USER_AGENTS) if include_optional_fields else None,
    )


def generate_missing_field_line(rng: random.Random) -> str:
    """Type 1 - Missing field: fewer fields than required by the schema."""

    ip = _random_ip(rng)
    timestamp = _random_timestamp(rng)
    request = _random_request(rng)
    status = str(rng.choice(STATUS_CODES))
    num_bytes = str(rng.randint(0, 98_304))

    variant = rng.randint(0, 2)
    if variant == 0:
        # Omit the trailing bytes field entirely.
        return '{ip} - - [{ts}] "{req}" {status}'.format(
            ip=ip, ts=timestamp, req=request, status=status
        )
    if variant == 1:
        # Omit authuser, leaving one hyphen instead of two.
        return '{ip} - [{ts}] "{req}" {status} {nbytes}'.format(
            ip=ip, ts=timestamp, req=request, status=status, nbytes=num_bytes
        )
    # Omit the timestamp field entirely.
    return '{ip} - - "{req}" {status} {nbytes}'.format(
        ip=ip, req=request, status=status, nbytes=num_bytes
    )


def generate_invalid_ipv4_line(rng: random.Random) -> str:
    """Type 2 - All fields exist, but the IP is not valid IPv4."""

    prefix = _random_prefix(rng)
    broken_ip = rng.choice(
        (
            "999.{}.{}".format(rng.randint(0, 255), rng.randint(0, 255)),
            prefix,                                    # only three octets
            "{}.{}.5".format(prefix, rng.randint(1, 254)),  # five octets
            "{}.abc".format(prefix),                   # non-numeric octet
            "{}.{}".format(prefix, rng.randint(256, 999)),  # octet above 255
            "-",                                       # not an IP address
        )
    )
    return _format_line(
        ip=broken_ip,
        timestamp=_random_timestamp(rng),
        request=_random_request(rng),
        status=str(rng.choice(STATUS_CODES)),
        num_bytes=str(rng.randint(0, 98_304)),
        referer=rng.choice(REFERERS),
        user_agent=rng.choice(USER_AGENTS),
    )


def generate_non_numeric_status_line(rng: random.Random) -> str:
    """Type 3 - Fields and IP are valid, but status is not an integer."""

    broken_status = rng.choice(("OK", "2xx", "-", "NaN", "20O", "ERROR", "null"))
    return _format_line(
        ip=_random_ip(rng),
        timestamp=_random_timestamp(rng),
        request=_random_request(rng),
        status=broken_status,
        num_bytes=str(rng.randint(0, 98_304)),
        referer=rng.choice(REFERERS),
        user_agent=rng.choice(USER_AGENTS),
    )


def generate_malformed_request_line(rng: random.Random) -> str:
    """Type 4 - Fields, IP, and status are valid, but the request is malformed."""

    endpoint = rng.choice(ENDPOINTS)
    method = rng.choice(HTTP_METHODS)
    broken_request = rng.choice(
        (
            "{}-only".format(method),                  # cannot split into three parts
            "{} {}".format(method, endpoint),          # missing HTTP version
            "{} {}".format(method.lower(), endpoint),  # no HTTP version, lowercase method
            "{} {} HTTP/1.1".format(method.lower(), endpoint),  # lowercase method
            "{} HTTP/1.1".format(endpoint),            # missing method
            "{} {} HTTP/1.1 extra".format(method, endpoint),    # extra component
            "{} {} HTTPS/1.1".format(method, endpoint),         # incorrect protocol name
        )
    )
    return _format_line(
        ip=_random_ip(rng),
        timestamp=_random_timestamp(rng),
        request=broken_request,
        status=str(rng.choice(STATUS_CODES)),
        num_bytes=str(rng.randint(0, 98_304)),
        referer=rng.choice(REFERERS),
        user_agent=rng.choice(USER_AGENTS),
    )


#: Map each malformed label to its generator function.
MALFORMED_GENERATORS = {
    "missing_field": generate_missing_field_line,
    "invalid_ipv4": generate_invalid_ipv4_line,
    "non_numeric_status": generate_non_numeric_status_line,
    "malformed_request": generate_malformed_request_line,
}


# --------------------------------------------------------------------------
# Generate the complete dataset
# --------------------------------------------------------------------------

def _split_counts(total: int) -> Tuple[int, Dict[str, int]]:
    """Split ``total`` into valid records and records for each malformed type.

    Allocation is DETERMINISTIC rather than random, so the 90/10 ratio remains
    exact. Any remainder from an uneven malformed-category split is assigned in
    taxonomy order, and the valid count keeps the total number of lines equal
    to ``total``.
    """

    malformed_total = round(total * (1 - VALID_RATIO))
    per_kind = malformed_total // len(MALFORMED_KINDS)
    counts = {kind: per_kind for kind in MALFORMED_KINDS}

    remainder = malformed_total - per_kind * len(MALFORMED_KINDS)
    for index in range(remainder):
        counts[MALFORMED_KINDS[index]] += 1

    valid_count = total - sum(counts.values())
    return valid_count, counts


def generate_lines(total: int, seed: int) -> List[str]:
    """Generate shuffled log lines with an exact 90/10 ratio."""

    rng = random.Random(seed)
    valid_count, malformed_counts = _split_counts(total)

    lines: List[str] = [generate_valid_line(rng) for _ in range(valid_count)]
    for kind, count in malformed_counts.items():
        generator = MALFORMED_GENERATORS[kind]
        lines.extend(generator(rng) for _ in range(count))

    rng.shuffle(lines)
    return lines


# --------------------------------------------------------------------------
# Post-generation self-check
# --------------------------------------------------------------------------

def _self_check(lines: List[str], expected_total: int) -> Dict[str, int]:
    """Validate generated data and raise ``AssertionError`` on contract violations."""

    counts: Dict[str, int] = {"valid": 0, "status_out_of_range": 0}
    counts.update({kind: 0 for kind in MALFORMED_KINDS})

    for line in lines:
        counts[classify_line(line)] += 1

    assert len(lines) == expected_total, (
        "Số dòng sinh ra ({}) khác với yêu cầu ({})".format(len(lines), expected_total)
    )
    assert counts["status_out_of_range"] == 0, (
        "Sinh ra status nằm ngoài 100..599 - không thuộc taxonomy 4 loại"
    )

    expected_valid, expected_malformed = _split_counts(expected_total)
    assert counts["valid"] == expected_valid, (
        "Số bản ghi hợp lệ ({}) khác kỳ vọng ({})".format(counts["valid"], expected_valid)
    )
    for kind, expected_count in expected_malformed.items():
        assert counts[kind] == expected_count, (
            "Loại lỗi '{}' đếm được {} nhưng kỳ vọng {}".format(
                kind, counts[kind], expected_count
            )
        )
        assert expected_count >= 1, "Loại lỗi '{}' phải xuất hiện ít nhất 1 lần".format(kind)

    # Every syntactically valid IP in the dataset must be covered by
    # IP_COUNTRY_MAP. The intentional 'invalid_ipv4' category is excluded.
    uncovered = set()
    for line in lines:
        if classify_line(line) == "invalid_ipv4":
            continue
        match = LOG_LINE_PATTERN.fullmatch(line.strip())
        if match is None:
            continue
        prefix = match.group("ip").rsplit(".", 1)[0]
        if prefix not in IP_COUNTRY_MAP:
            uncovered.add(prefix)
    assert not uncovered, "IP_COUNTRY_MAP chưa phủ các tiền tố: {}".format(sorted(uncovered))

    return counts


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def _parse_args(argv: List[str] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sinh raw_logs.txt giả lập với tỉ lệ 90% hợp lệ / 10% lỗi."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / DEFAULT_OUTPUT_RELPATH,
        help="Đường dẫn file output (mặc định: %(default)s).",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=DEFAULT_TOTAL_RECORDS,
        help="Tổng số dòng log cần sinh (mặc định: %(default)s).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="Seed ngẫu nhiên để tái lập kết quả (mặc định: %(default)s).",
    )
    return parser.parse_args(argv)


def main(argv: List[str] = None) -> int:
    args = _parse_args(argv)

    if args.count <= 0:
        raise SystemExit("--count phải là số nguyên dương.")

    lines = generate_lines(args.count, args.seed)
    counts = _self_check(lines, args.count)

    output_path: Path = args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="\n") as handle:
        for line in lines:
            handle.write(line + "\n")

    valid_count = counts["valid"]
    malformed_count = args.count - valid_count

    print("Đã ghi {} dòng vào {}".format(args.count, output_path))
    print("Seed: {}".format(args.seed))
    print(
        "Hợp lệ : {:>6} ({:.2f}%)".format(valid_count, valid_count / args.count * 100)
    )
    print(
        "Lỗi    : {:>6} ({:.2f}%)".format(
            malformed_count, malformed_count / args.count * 100
        )
    )
    for kind in MALFORMED_KINDS:
        print("  - {:<20}: {:>5}".format(kind, counts[kind]))
    print("IP_COUNTRY_MAP: {} tiền tố /24, {} quốc gia".format(
        len(IP_COUNTRY_MAP), len(set(IP_COUNTRY_MAP.values()))
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
