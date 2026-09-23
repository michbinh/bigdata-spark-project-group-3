"""Sinh dữ liệu log truy cập web giả lập cho pipeline Spark (Task 2.2 - ID2).

================================================================================
ĐỊNH DẠNG LOG (hợp đồng dùng chung với core parser của ID1 trong
``rdd_processing.py``). Mỗi bản ghi là MỘT dòng theo Combined Log Format:

    <ip> <ident> <authuser> [<timestamp>] "<method> <endpoint> HTTP/<ver>" <status> <bytes> "<referer>" "<user_agent>"

Ví dụ một dòng hợp lệ đầy đủ::

    192.168.10.24 - - [10/Oct/2000:13:55:36 +0700] "GET /products?id=7 HTTP/1.1" 200 1234 "https://example.com/" "Mozilla/5.0"

Ví dụ một dòng hợp lệ dạng rút gọn (Common Log Format - bỏ 2 trường cuối)::

    192.168.10.24 - - [10/Oct/2000:13:55:36 +0700] "GET /products?id=7 HTTP/1.1" 200 1234

Quy ước từng trường:

===============  ============================================================
Trường           Mô tả
===============  ============================================================
``ip``           Địa chỉ IPv4 dạng dotted-quad. Đây là khóa để tra cứu quốc gia.
``ident``        Luôn là ``-`` (không dùng identd).
``authuser``     Luôn là ``-`` (không có HTTP auth).
``timestamp``    Bọc trong ``[]``, dạng ``dd/Mon/yyyy:HH:MM:SS +0700``.
                 Tên tháng viết tiếng Anh 3 ký tự, KHÔNG phụ thuộc locale.
``request``      Bọc trong ``""``, gồm đúng 3 phần cách nhau bởi khoảng trắng:
                 method viết HOA (``[A-Z]+``), endpoint không chứa khoảng
                 trắng, và phiên bản ``HTTP/<major>[.<minor>]``.
``status``       Số nguyên HTTP status, nằm trong khoảng 100..599.
``bytes``        Số nguyên, kích thước response tính theo byte.
``referer``      Tùy chọn, bọc trong ``""``.
``user_agent``   Tùy chọn, bọc trong ``""``. Nếu có ``referer`` thì phải có
                 ``user_agent`` và ngược lại - hai trường này đi theo cặp.
===============  ============================================================

Dấu phân cách giữa các trường là khoảng trắng. Các trường ``timestamp``,
``request``, ``referer``, ``user_agent`` có thể chứa khoảng trắng bên trong nên
được bao bởi ``[]`` hoặc ``""`` để vẫn tách được.

================================================================================
TỈ LỆ SINH: đúng 90% bản ghi hợp lệ / 10% bản ghi lỗi. Con số được chia CỐ ĐỊNH
(không ngẫu nhiên) nên tỉ lệ luôn chính xác tuyệt đối, không phụ thuộc seed.
Phần 10% lỗi chia đều cho 4 loại trong taxonomy bên dưới.

TAXONOMY BẢN GHI LỖI (canonical - CÓ THỨ TỰ ƯU TIÊN, mutually exclusive).
Mỗi bản ghi lỗi được phân loại bằng cách kiểm tra theo đúng thứ tự sau và dừng
ở loại ĐẦU TIÊN khớp:

    1. ``missing_field``      - Thiếu trường: số trường tách được ít hơn schema.
    2. ``invalid_ipv4``       - Đủ trường, nhưng trường IP không đúng IPv4.
    3. ``non_numeric_status`` - Đủ trường, IP hợp lệ, status không parse được
                                thành số nguyên.
    4. ``malformed_request``  - Đủ trường, IP hợp lệ, status hợp lệ, nhưng
                                method/endpoint/phiên bản HTTP sai cấu trúc.

Bản ghi lỗi được sinh ra CÓ CHỦ ĐÍCH và không được "làm sạch" - chúng là đầu
vào để kiểm chứng Accumulator đếm bản ghi hỏng ở Task 2.4.

================================================================================
CÁCH CHẠY - từ thư mục gốc của repo, không cần tham số, không cần thư viện
ngoài stdlib (script KHÔNG dùng PySpark)::

    python3 src/generate_logs.py

Mặc định ghi ra ``data/raw_logs.txt``. Đường dẫn được suy ra từ vị trí của
chính file script này nên chạy từ thư mục nào cũng cho kết quả giống nhau -
KHÔNG hardcode đường dẫn tuyệt đối.
"""

from __future__ import annotations

import argparse
import ipaddress
import random
import re
from pathlib import Path
from typing import Dict, List, Tuple


# --------------------------------------------------------------------------
# Hằng số cấu hình
# --------------------------------------------------------------------------

#: Tổng số bản ghi sinh ra mặc định. Chia hết cho 40 để 90/10 và 4 loại lỗi
#: đều chia trọn vẹn, không phát sinh sai số làm tròn.
DEFAULT_TOTAL_RECORDS = 10_000

#: Tỉ lệ bản ghi hợp lệ theo yêu cầu đề bài.
VALID_RATIO = 0.9

#: Seed mặc định để kết quả tái lập được (ID7 QA clean-clone chạy lại ra y hệt).
DEFAULT_SEED = 20260922

#: Thư mục gốc của repo. Script nằm trong ``src/`` nên gốc là thư mục cha.
REPO_ROOT = Path(__file__).resolve().parent.parent

#: Đường dẫn output mặc định, tương đối so với gốc repo. Vị trí này do ID6 quy
#: định trong README và được giữ lại tường minh bởi ``!data/raw_logs.txt``
#: trong ``.gitignore``.
DEFAULT_OUTPUT_RELPATH = Path("data") / "raw_logs.txt"


# --------------------------------------------------------------------------
# Bảng tra cứu IP -> quốc gia (interface dùng chung với Broadcast ở Task 2.4)
# --------------------------------------------------------------------------

#: Khóa là tiền tố /24 (ba octet đầu của địa chỉ IPv4), giá trị là tên quốc gia.
#:
#: Cách tra cứu phía consumer (Task 2.4 - Broadcast enrichment)::
#:
#:     prefix = record["ip"].rsplit(".", 1)[0]
#:     country = broadcast_map.value.get(prefix)
#:
#: Dùng /24 thay vì từng IP đầy đủ để bảng đủ nhỏ cho Broadcast mà vẫn phủ
#: được toàn bộ 254 địa chỉ host trong mỗi dải.
#:
#: Toàn bộ dải IP dưới đây nằm trong không gian private (RFC 1918) nên đây là
#: dữ liệu tổng hợp, không trỏ tới hạ tầng thật của bất kỳ tổ chức nào.
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

#: Trọng số lưu lượng theo tiền tố, để kết quả Top 10 Countries có phân bố
#: lệch rõ ràng thay vì đều nhau (giúp kiểm chứng thứ hạng dễ hơn).
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
# Kho giá trị để sinh bản ghi hợp lệ
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
# Mẫu nhận dạng dùng cho bước tự kiểm tra (self-check)
# --------------------------------------------------------------------------

#: Cấu trúc dòng log theo đúng hợp đồng mô tả ở docstring đầu file. Mẫu này
#: chỉ phục vụ self-check của chính generator; nguồn chân lý khi xử lý dữ liệu
#: là ``LOG_PATTERN`` trong ``rdd_processing.py`` của ID1 (hai mẫu phải tương
#: đương nhau về cấu trúc).
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

#: Nhãn của 4 loại lỗi, xếp theo đúng thứ tự ưu tiên của taxonomy.
MALFORMED_KINDS: Tuple[str, ...] = (
    "missing_field",
    "invalid_ipv4",
    "non_numeric_status",
    "malformed_request",
)


# --------------------------------------------------------------------------
# Phân loại bản ghi theo taxonomy (thứ tự ưu tiên, mutually exclusive)
# --------------------------------------------------------------------------

def classify_line(line: str) -> str:
    """Phân loại một dòng log vào ĐÚNG MỘT nhãn theo taxonomy.

    Trả về ``"valid"`` hoặc một trong 4 nhãn của :data:`MALFORMED_KINDS`.
    Việc kiểm tra tuân thủ nghiêm ngặt thứ tự ưu tiên 1 -> 2 -> 3 -> 4 và dừng
    ở loại đầu tiên khớp, nên kết quả luôn loại trừ lẫn nhau.

    Nhãn ``"status_out_of_range"`` nằm NGOÀI taxonomy và chỉ xuất hiện nếu
    generator có lỗi; hàm :func:`_self_check` khẳng định nó không bao giờ xảy ra.
    """

    match = LOG_LINE_PATTERN.fullmatch(line.strip())

    # (1) Thiếu trường: không tách đủ số trường mà schema yêu cầu.
    if match is None:
        return "missing_field"

    # (2) IPv4 sai định dạng.
    try:
        ipaddress.IPv4Address(match.group("ip"))
    except ipaddress.AddressValueError:
        return "invalid_ipv4"

    # (3) Status code không phải số nguyên.
    try:
        status_code = int(match.group("status"))
    except ValueError:
        return "non_numeric_status"

    if not 100 <= status_code <= 599:
        return "status_out_of_range"

    # (4) Request sai cấu trúc method / endpoint / phiên bản HTTP.
    if REQUEST_PATTERN.fullmatch(match.group("request")) is None:
        return "malformed_request"

    return "valid"


# --------------------------------------------------------------------------
# Sinh từng thành phần của bản ghi
# --------------------------------------------------------------------------

def _random_prefix(rng: random.Random) -> str:
    """Chọn ngẫu nhiên một tiền tố /24 theo trọng số lưu lượng."""

    prefixes = list(PREFIX_WEIGHTS)
    weights = [PREFIX_WEIGHTS[prefix] for prefix in prefixes]
    return rng.choices(prefixes, weights=weights, k=1)[0]


def _random_ip(rng: random.Random) -> str:
    """Sinh một địa chỉ IPv4 hợp lệ nằm trong một dải /24 đã khai báo."""

    return "{}.{}".format(_random_prefix(rng), rng.randint(1, 254))


def _random_timestamp(rng: random.Random) -> str:
    """Sinh timestamp dạng ``dd/Mon/yyyy:HH:MM:SS +0700`` (không phụ thuộc locale)."""

    return "{day:02d}/{month}/{year}:{hour:02d}:{minute:02d}:{second:02d} +0700".format(
        day=rng.randint(1, 28),
        month=rng.choice(MONTH_ABBREVIATIONS),
        year=2026,
        hour=rng.randint(0, 23),
        minute=rng.randint(0, 59),
        second=rng.randint(0, 59),
    )


def _random_request(rng: random.Random) -> str:
    """Sinh phần request hợp lệ: ``METHOD endpoint HTTP/x.y``."""

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
    """Ghép các thành phần thành một dòng log hoàn chỉnh."""

    line = '{ip} - - [{ts}] "{req}" {status} {nbytes}'.format(
        ip=ip, ts=timestamp, req=request, status=status, nbytes=num_bytes
    )
    if referer is not None and user_agent is not None:
        line += ' "{}" "{}"'.format(referer, user_agent)
    return line


def generate_valid_line(rng: random.Random) -> str:
    """Sinh một bản ghi HỢP LỆ đầy đủ theo hợp đồng định dạng."""

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
    """Loại 1 - Thiếu trường: số trường tách được ít hơn schema yêu cầu."""

    ip = _random_ip(rng)
    timestamp = _random_timestamp(rng)
    request = _random_request(rng)
    status = str(rng.choice(STATUS_CODES))
    num_bytes = str(rng.randint(0, 98_304))

    variant = rng.randint(0, 2)
    if variant == 0:
        # Thiếu hẳn trường bytes ở cuối.
        return '{ip} - - [{ts}] "{req}" {status}'.format(
            ip=ip, ts=timestamp, req=request, status=status
        )
    if variant == 1:
        # Thiếu trường authuser (chỉ còn một dấu gạch ngang thay vì hai).
        return '{ip} - [{ts}] "{req}" {status} {nbytes}'.format(
            ip=ip, ts=timestamp, req=request, status=status, nbytes=num_bytes
        )
    # Thiếu hẳn trường timestamp.
    return '{ip} - - "{req}" {status} {nbytes}'.format(
        ip=ip, req=request, status=status, nbytes=num_bytes
    )


def generate_invalid_ipv4_line(rng: random.Random) -> str:
    """Loại 2 - Đủ trường, nhưng trường IP không đúng định dạng IPv4."""

    prefix = _random_prefix(rng)
    broken_ip = rng.choice(
        (
            "999.{}.{}".format(rng.randint(0, 255), rng.randint(0, 255)),
            prefix,                                    # chỉ có 3 octet
            "{}.{}.5".format(prefix, rng.randint(1, 254)),  # thừa thành 5 octet
            "{}.abc".format(prefix),                   # octet không phải số
            "{}.{}".format(prefix, rng.randint(256, 999)),  # octet vượt 255
            "-",                                       # hoàn toàn không phải IP
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
    """Loại 3 - Đủ trường, IP hợp lệ, status không parse được thành số nguyên."""

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
    """Loại 4 - Đủ trường, IP và status hợp lệ, nhưng request sai cấu trúc."""

    endpoint = rng.choice(ENDPOINTS)
    method = rng.choice(HTTP_METHODS)
    broken_request = rng.choice(
        (
            "{}-only".format(method),                  # không tách được 3 phần
            "{} {}".format(method, endpoint),          # thiếu phiên bản HTTP
            "{} {}".format(method.lower(), endpoint),  # thiếu HTTP + method thường
            "{} {} HTTP/1.1".format(method.lower(), endpoint),  # method viết thường
            "{} HTTP/1.1".format(endpoint),            # thiếu method
            "{} {} HTTP/1.1 extra".format(method, endpoint),    # thừa thành phần
            "{} {} HTTPS/1.1".format(method, endpoint),         # sai tên giao thức
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


#: Ánh xạ nhãn lỗi -> hàm sinh tương ứng.
MALFORMED_GENERATORS = {
    "missing_field": generate_missing_field_line,
    "invalid_ipv4": generate_invalid_ipv4_line,
    "non_numeric_status": generate_non_numeric_status_line,
    "malformed_request": generate_malformed_request_line,
}


# --------------------------------------------------------------------------
# Sinh toàn bộ tập dữ liệu
# --------------------------------------------------------------------------

def _split_counts(total: int) -> Tuple[int, Dict[str, int]]:
    """Chia ``total`` thành số bản ghi hợp lệ và số bản ghi cho từng loại lỗi.

    Việc chia là CỐ ĐỊNH (không ngẫu nhiên) nên tỉ lệ 90/10 luôn chính xác
    tuyệt đối. Phần dư do chia không hết được dồn vào nhóm hợp lệ để tổng số
    dòng luôn đúng bằng ``total``.
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
    """Sinh danh sách dòng log đã trộn ngẫu nhiên, với tỉ lệ 90/10 chính xác."""

    rng = random.Random(seed)
    valid_count, malformed_counts = _split_counts(total)

    lines: List[str] = [generate_valid_line(rng) for _ in range(valid_count)]
    for kind, count in malformed_counts.items():
        generator = MALFORMED_GENERATORS[kind]
        lines.extend(generator(rng) for _ in range(count))

    rng.shuffle(lines)
    return lines


# --------------------------------------------------------------------------
# Tự kiểm tra sau khi sinh
# --------------------------------------------------------------------------

def _self_check(lines: List[str], expected_total: int) -> Dict[str, int]:
    """Kiểm chứng tập dữ liệu vừa sinh; ném ``AssertionError`` nếu sai hợp đồng."""

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

    # Mọi IP đúng cú pháp xuất hiện trong dữ liệu đều phải được IP_COUNTRY_MAP phủ.
    # Riêng loại lỗi 'invalid_ipv4' cố tình mang IP hỏng nên được loại trừ.
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
# Điểm vào
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
