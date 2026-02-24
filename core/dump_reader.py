import re
from pathlib import Path

from config import DEFAULT_ENCODING, DUMP_DIR, DUMP_EXTENSIONS, TABLE_GROUPS
from utils.helpers import remove_table_prefix
from utils.helpers import (
    detect_table_prefix,
    get_file_size_mb,
    is_gzip_file,
    list_dump_files,
    open_dump_file,
)


class DumpReader:
    """خواندن و پارس فایل دامپ MySQL - فشرده یا عادی."""

    # فقط این نوع دستورات را پردازش می‌کنیم (VIEW, PROCEDURE و ... نادیده گرفته می‌شوند)
    WANTED_STATEMENTS = ("CREATE TABLE", "INSERT INTO")

    def __init__(self, dump_dir: str | Path = None):
        self.dump_dir = Path(dump_dir or DUMP_DIR)

    def list_files(self) -> list[dict]:
        """لیست فایل‌های دامپ موجود در پوشه."""
        return list_dump_files(self.dump_dir, DUMP_EXTENSIONS)

    def select_file(self, index: int) -> Path | None:
        """انتخاب فایل بر اساس شماره در لیست."""
        files = self.list_files()
        if 0 <= index < len(files):
            return files[index]["path"]
        return None

    def get_info(self, dump_path: str | Path) -> dict:
        path = Path(dump_path)
        if not path.exists():
            raise FileNotFoundError(f"فایل یافت نشد: {path}")

        encoding = DEFAULT_ENCODING  # دامپ MySQL/وردپرس: UTF-8
        size_mb = get_file_size_mb(path)
        return {
            "path": str(path),
            "name": path.name,
            "size_mb": round(size_mb, 2),
            "encoding": encoding,
            "compressed": is_gzip_file(path),
        }

    def _extract_table_name(self, sql: str, keyword: str) -> str | None:
        """استخراج نام جدول از دستور CREATE TABLE یا INSERT INTO."""
        if keyword == "CREATE TABLE":
            pattern = r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`\"]?(\w+)[`\"]?"
        else:
            pattern = r"INSERT\s+INTO\s+[`\"]?(\w+)[`\"]?"
        m = re.search(pattern, sql, re.IGNORECASE)
        return m.group(1) if m else None

    _TABLE_NAME_PATTERN = re.compile(
        r"(?:CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?|INSERT\s+INTO\s+)[`\"]?(\w+)[`\"]?",
        re.IGNORECASE,
    )

    def _first_pass_extract_tables(self, dump_path: Path, encoding: str) -> list[str]:
        """پاس اول: استخراج نام جدول‌ها برای تشخیص پیشوند."""
        table_names: list[str] = []
        seen = set()

        for line in open_dump_file(dump_path, encoding):
            for m in self._TABLE_NAME_PATTERN.finditer(line):
                name = m.group(1)
                if name not in seen:
                    seen.add(name)
                    table_names.append(name)
        return table_names

    def detect_prefix(self, dump_path: str | Path, encoding: str = None) -> str:
        """تشخیص خودکار پیشوند جدول‌ها از روی فایل دامپ."""
        path = Path(dump_path)
        # دامپ MySQL/وردپرس همیشه UTF-8 است؛ استفاده از encoding دیگر باعث خرابی متن فارسی می‌شود
        enc = encoding or DEFAULT_ENCODING
        table_names = self._first_pass_extract_tables(path, enc)
        return detect_table_prefix(table_names)

    def get_complete_groups(
        self,
        dump_path: str | Path,
        prefix: str = "",
        table_groups: dict[str, list[str]] = None,
    ) -> list[str]:
        """
        بر اساس جداول موجود در دامپ، گروه‌هایی که به طور کامل در دامپ هستند را برمی‌گرداند.
        جداول دامپ با حذف پیشوند نرمال می‌شوند تا با لیست (بدون پیشوند) مقایسه شوند.
        """
        path = Path(dump_path)
        enc = DEFAULT_ENCODING  # دامپ MySQL/وردپرس: UTF-8
        raw_tables = self._first_pass_extract_tables(path, enc)
        dump_tables = {remove_table_prefix(t, prefix) for t in raw_tables}
        groups = table_groups or TABLE_GROUPS
        complete = []
        for group_name, expected_tables in groups.items():
            if all(t in dump_tables for t in expected_tables):
                complete.append(group_name)
        return complete

    def _split_statements(self, text: str) -> tuple[list[str], str]:
        """تقسیم متن به دستورات SQL. برمی‌گرداند (لیست دستورات، باقیمانده ناقص)."""
        parts = []
        current = []
        in_str = False
        q = None
        i = 0
        n = len(text)

        while i < n:
            c = text[i]
            if not in_str:
                if c in ("'", '"', "`"):
                    in_str = True
                    q = c
                    current.append(c)
                elif c == ";":
                    stmt = "".join(current).strip()
                    if stmt:
                        parts.append(stmt)
                    current = []
                elif c == "\\" and i + 1 < n:
                    current.append(c)
                    current.append(text[i + 1])
                    i += 1
                else:
                    current.append(c)
            else:
                if c == "\\" and i + 1 < n:
                    current.append(c)
                    current.append(text[i + 1])
                    i += 1
                elif c == q:
                    in_str = False
                    q = None
                    current.append(c)
                else:
                    current.append(c)
            i += 1

        remainder = "".join(current)
        return parts, remainder

    def read_statements(self, dump_path: str | Path, encoding: str = None):
        """
        خواندن دستورات CREATE TABLE و INSERT از فایل دامپ.
        فقط این دو نوع را yield می‌کند؛ VIEW، PROCEDURE و غیره نادیده گرفته می‌شوند.
        """
        path = Path(dump_path)
        if not path.exists():
            raise FileNotFoundError(f"فایل یافت نشد: {path}")

        enc = encoding or DEFAULT_ENCODING  # دامپ MySQL/وردپرس: UTF-8
        buffer = ""

        for line in open_dump_file(path, enc):
            buffer += line
            if ";" not in buffer:
                continue

            parts, remainder = self._split_statements(buffer)
            buffer = remainder

            for stmt in parts:
                stmt = stmt.strip()
                if not stmt:
                    continue
                lines = stmt.split("\n")
                while lines and lines[0].strip().startswith("--"):
                    lines.pop(0)
                stmt = "\n".join(lines).strip()
                if not stmt:
                    continue
                upper = stmt.upper()
                if upper.startswith("CREATE TABLE") or upper.startswith("INSERT INTO"):
                    yield stmt
