"""
تبدیل دستورات MySQL به SQLite.
برای فایل‌های بزرگ (تا ۱ گیگ) بهینه شده - بدون بارگذاری کل فایل در حافظه.
"""
import re


class MySQLToSQLiteConverter:
    """تبدیل دستورات MySQL به SQLite."""

    # الگو برای استخراج نام جدول از CREATE TABLE و INSERT
    _CREATE_TABLE_PATTERN = re.compile(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`\"]?(\w+)[`\"]?",
        re.IGNORECASE,
    )
    _INSERT_TABLE_PATTERN = re.compile(
        r"INSERT\s+INTO\s+[`\"]?(\w+)[`\"]?",
        re.IGNORECASE,
    )

    def _extract_table_name(self, sql: str, stmt_type: str) -> str | None:
        if stmt_type == "CREATE":
            m = self._CREATE_TABLE_PATTERN.search(sql)
        else:
            m = self._INSERT_TABLE_PATTERN.search(sql)
        return m.group(1) if m else None

    def _convert_create_table(self, sql: str) -> str:
        """تبدیل CREATE TABLE از MySQL به SQLite."""
        # حذف ENGINE، CHARSET، COLLATE و ...
        sql = re.sub(r"\s+ENGINE\s*=\s*\w+", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\s+DEFAULT\s+CHARSET\s*=\s*\w+", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\s+COLLATE\s*=\s*\w+", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\s+AUTO_INCREMENT\s*=\s*\d+", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\s*,\s*\)", "\n)", sql)  # حذف کامای آخر قبل از )

        # تبدیل بک‌تیک به دابل کوتی
        sql = re.sub(r"`([^`]+)`", r'"\1"', sql)

        # تبدیل انواع داده MySQL به SQLite (با فاصله بعد از نوع برای جلوگیری از چسبیدن به NOT NULL)
        sql = re.sub(
            r"\b(INT|BIGINT|TINYINT|SMALLINT|MEDIUMINT|INTEGER)\s*(\(\d+\))?\s*(UNSIGNED)?\s*(ZEROFILL)?\b",
            "INTEGER ",
            sql,
            flags=re.IGNORECASE,
        )
        sql = re.sub(
            r"\b(VARCHAR|CHAR|TEXT|TINYTEXT|MEDIUMTEXT|LONGTEXT|BLOB|TINYBLOB|MEDIUMBLOB|LONGBLOB)\s*(\(\d+\))?\s*(CHARACTER\s+SET\s+\w+)?\s*(COLLATE\s+\w+)?\b",
            "TEXT ",
            sql,
            flags=re.IGNORECASE,
        )
        sql = re.sub(
            r"\b(DECIMAL|NUMERIC|FLOAT|DOUBLE|REAL)\s*(\([^)]+\))?\s*(UNSIGNED)?\b",
            "REAL ",
            sql,
            flags=re.IGNORECASE,
        )
        sql = re.sub(
            r"\b(DATETIME|DATE|TIMESTAMP|TIME|YEAR)\s*(\(\d+\))?\b",
            "TEXT ",
            sql,
            flags=re.IGNORECASE,
        )
        sql = re.sub(
            r"\bENUM\s*\([^)]+\)\s*(CHARACTER\s+SET\s+\w+)?\s*(COLLATE\s+\w+)?\b",
            "TEXT ",
            sql,
            flags=re.IGNORECASE,
        )
        sql = re.sub(
            r"\bSET\s*\([^)]+\)\s*(CHARACTER\s+SET\s+\w+)?\s*(COLLATE\s+\w+)?\b",
            "TEXT ",
            sql,
            flags=re.IGNORECASE,
        )

        # current_timestamp() -> CURRENT_TIMESTAMP برای SQLite
        sql = re.sub(r"(?:CURRENT_TIMESTAMP|current_timestamp)\s*\(\s*\)", "CURRENT_TIMESTAMP", sql, flags=re.IGNORECASE)
        # حذف UNSIGNED
        sql = re.sub(r"\s+UNSIGNED\b", "", sql, flags=re.IGNORECASE)
        # حذف AUTO_INCREMENT از ستون (SQLite فقط با INTEGER PRIMARY KEY قبول می‌کند)
        sql = re.sub(r"\s+AUTO_INCREMENT\b", "", sql, flags=re.IGNORECASE)
        # حذف KEY و INDEX ثانویه (PRIMARY KEY در ستون یا جداگانه نگه داشته می‌شود)
        sql = re.sub(r",\s*KEY\s+[`\"]?\w+[`\"]?\s*\([^)]+\)(?:\s+USING\s+\w+)?", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r",\s*UNIQUE\s+KEY\s+[`\"]?\w+[`\"]?\s*\([^)]+\)(?:\s+USING\s+\w+)?", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r",\s*FULLTEXT\s+(?:KEY|INDEX)\s+[^)]+\)", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r",\s*CONSTRAINT\s+[^,]+\s+FOREIGN\s+KEY\s+[^)]+\)[^,]*(?:,|\)\s*;?)", ",", sql, flags=re.IGNORECASE)
        # حذف کامای اضافی قبل از )
        sql = re.sub(r"\s*,\s*\)", "\n)", sql)
        # حذف ) اضافی وقتی سه تا یا بیشتر ) پشت سر هم در انتها باشد
        sql = re.sub(r"\)\s*\)\s*\)\s*$", ")\n)", sql)

        return sql

    def _convert_insert(self, sql: str) -> str:
        """تبدیل INSERT از MySQL به SQLite."""
        # \N در MySQL یعنی NULL
        sql = re.sub(r"\\N(?=[,\s\)])", "NULL", sql)
        # escape های MySQL: \' -> '' برای SQLite
        sql = sql.replace("\\'", "''")
        sql = sql.replace('\\"', '"')
        sql = sql.replace("\\n", "\n")
        sql = sql.replace("\\r", "\r")
        # بک‌تیک به دابل کوتی
        sql = re.sub(r"`([^`]+)`", r'"\1"', sql)
        return sql

    def convert(self, sql: str, target_table: str) -> str:
        """تبدیل دستورات MySQL به SQLite با نام جدول هدف."""
        upper = sql.upper().strip()
        if upper.startswith("CREATE TABLE"):
            # جایگزینی نام جدول با نام بدون پیشوند
            converted = self._convert_create_table(sql)
            # نام جدول در CREATE باید با target_table جایگزین شود
            converted = re.sub(
                r'(CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?)"[^"]+"',
                rf'\1"{target_table}"',
                converted,
                count=1,
                flags=re.IGNORECASE,
            )
            return converted
        if upper.startswith("INSERT INTO"):
            converted = self._convert_insert(sql)
            converted = re.sub(
                r'(INSERT\s+INTO\s+)"[^"]+"',
                rf'\1"{target_table}"',
                converted,
                count=1,
                flags=re.IGNORECASE,
            )
            return converted
        return sql
