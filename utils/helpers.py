import gzip
import os
from collections import Counter
from pathlib import Path

import chardet
import jdatetime


def detect_file_encoding(file_path: str | Path) -> str:
    with open(file_path, "rb") as f:
        raw = f.read(100_000)
    result = chardet.detect(raw)
    return result.get("encoding", "utf-8") or "utf-8"


def ensure_dir(path: str | Path) -> Path:
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_file_size_mb(file_path: str | Path) -> float:
    return os.path.getsize(file_path) / (1024 * 1024)


def remove_table_prefix(table_name: str, prefix: str) -> str:
    if prefix and table_name.startswith(prefix):
        return table_name[len(prefix) :]
    return table_name


def is_gzip_file(file_path: str | Path) -> bool:
    path = Path(file_path)
    return path.suffix == ".gz" or path.name.endswith(".sql.gz")


def detect_gzip_encoding(file_path: str | Path, sample_size: int = 500_000) -> str:
    """تشخیص encoding فایل فشرده gzip با نمونه‌گیری از محتوای decompress شده."""
    path = Path(file_path)
    if not is_gzip_file(path):
        return "utf-8"
    try:
        with gzip.open(path, "rb") as f:
            raw = f.read(sample_size)
        result = chardet.detect(raw)
        enc = result.get("encoding") or "utf-8"
        return enc if enc else "utf-8"
    except Exception:
        return "utf-8"


def list_dump_files(dump_dir: str | Path, extensions: tuple = (".sql", ".gz", ".sql.gz")) -> list[dict]:
    """لیست فایل‌های دامپ در پوشه با جزئیات."""
    dump_dir = Path(dump_dir)
    if not dump_dir.exists():
        return []

    result = []
    for f in sorted(dump_dir.iterdir()):
        if not f.is_file():
            continue
        if f.suffix in extensions or f.name.endswith(".sql.gz"):
            result.append({
                "name": f.name,
                "path": f,
                "size_mb": round(get_file_size_mb(f), 2),
                "compressed": is_gzip_file(f),
            })
    return result


def detect_table_prefix(table_names: list[str]) -> str:
    """
    از روی لیست نام جدول‌ها، رایج‌ترین پیشوند را پیدا می‌کند.
    مثلاً wp_users, wp_posts, wp_options -> wp_
    """
    if not table_names:
        return ""

    prefixes: list[str] = []
    for name in table_names:
        idx = name.find("_")
        if idx > 0:
            prefixes.append(name[: idx + 1])
        else:
            prefixes.append("")

    if not prefixes:
        return ""

    counter = Counter(p for p in prefixes if p)
    if not counter:
        return ""

    most_common = counter.most_common(1)[0]
    if most_common[1] >= 2:
        return most_common[0]
    return ""


def get_shamsi_date() -> str:
    """برگرداندن تاریخ فعلی به صورت رشته."""
    now = jdatetime.datetime.now()
    return now.strftime("%Y/%m/%d")


def create_output_folder(base_dir: Path, folder_name: str) -> Path:
    """
    داخل base_dir یک پوشه با نام folder_name می‌سازد.
    اگر وجود داشت، عدد به انتها اضافه می‌کند (مثلاً folder_1, folder_2).
    """
    base_dir = Path(base_dir)
    base_dir.mkdir(parents=True, exist_ok=True)

    # نام تمیز برای پوشه (حذف کاراکترهای نامعتبر)
    safe_name = "".join(c for c in folder_name if c.isalnum() or c in "_-") or "output"

    target = base_dir / safe_name
    if not target.exists():
        target.mkdir(parents=True, exist_ok=True)
        return target

    n = 1
    while True:
        candidate = base_dir / f"{safe_name}_{n}"
        if not candidate.exists():
            candidate.mkdir(parents=True, exist_ok=True)
            return candidate
        n += 1


def _format_table_stats(
    table_row_counts: dict[str, int],
    table_groups: dict[str, list[str]],
    complete_groups: list[str],
) -> list[str]:
    """فرمت جدول برای نمایش آمار دیتابیس موقت."""
    rows = []
    # نگاشت جدول -> گروه
    table_to_group = {}
    for group_name in complete_groups:
        if group_name in table_groups:
            for table in table_groups[group_name]:
                table_to_group[table] = group_name
    # جداول مشتق‌شده در گروه wp نمایش داده شوند
    if "wp" in complete_groups and "customer_purchases" in table_row_counts:
        table_to_group["customer_purchases"] = "wp"

    # سطر هدر
    rows.append("گروه | جدول | تعداد رکورد")
    rows.append("-" * 45)

    for table, count in sorted(table_row_counts.items()):
        group = table_to_group.get(table, "-")
        rows.append(f"{group} | {table} | {count}")

    return rows


def write_output_readme(
    folder: Path,
    dump_name: str,
    size_mb: float,
    complete_groups: list[str] = None,
    table_groups: dict[str, list[str]] = None,
    table_row_counts: dict[str, int] = None,
    rfm_from_shamsi_date: str = "0",
) -> Path:
    """فایل README داخل پوشه خروجی با تاریخ، نام فایل، حجم، وضعیت لیست‌ها و آمار دیتابیس موقت."""
    readme_path = folder / "README.txt"
    shamsi_date = get_shamsi_date()
    lines = [
        f"تاریخ: {shamsi_date}",
        f"نام فایل: {dump_name}",
        f"حجم فایل: {size_mb} MB",
    ]
    if table_groups and complete_groups is not None:
        lines.append("")
        lines.append("بررسی لیست‌ها:")
        for group_name in table_groups:
            status = "detect" if group_name in complete_groups else "not found"
            lines.append(f"{group_name}: {status}")

    lines.append("")
    if str(rfm_from_shamsi_date).strip() and str(rfm_from_shamsi_date).strip() != "0":
        lines.append(f"RFM از تاریخ شمسی: {rfm_from_shamsi_date}")
    else:
        lines.append("RFM از تاریخ شمسی: از ابتدا")

    if table_row_counts and complete_groups and table_groups:
        lines.append("")
        lines.append("دیتابیس موقت:")
        lines.append(f"  تعداد جدول‌ها: {len(table_row_counts)}")
        lines.append("")
        lines.extend("  " + row for row in _format_table_stats(table_row_counts, table_groups, complete_groups))

    content = "\n".join(lines) + "\n"
    readme_path.write_text(content, encoding="utf-8")
    return readme_path


def open_dump_file(file_path: str | Path, encoding: str = "utf-8"):
    """
    فایل دامپ را باز می‌کند (فشرده یا عادی).
    یک generator از خطوط برمی‌گرداند.
    """
    path = Path(file_path)
    if is_gzip_file(path):
        with gzip.open(path, "rt", encoding=encoding, errors="replace") as f:
            yield from f
    else:
        with open(path, "r", encoding=encoding, errors="replace") as f:
            yield from f
