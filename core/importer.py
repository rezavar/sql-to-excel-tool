"""
وارد کردن جداول از دامپ MySQL به دیتابیس موقت SQLite.
بهینه برای فایل‌های بزرگ (تا ۱ گیگ) - استریم و پردازش بدون بارگذاری کل فایل.
"""
import sqlite3
from pathlib import Path

from config import TABLE_GROUPS
from core.converter import MySQLToSQLiteConverter
from core.db_manager import SQLiteManager
from core.dump_reader import DumpReader
from utils.helpers import remove_table_prefix


class DumpImporter:
    """وارد کردن جداول انتخاب‌شده از دامپ MySQL به SQLite."""

    def __init__(
        self,
        db_path,
        dump_reader: DumpReader = None,
        converter: MySQLToSQLiteConverter = None,
    ):
        self.db_path = db_path
        self.reader = dump_reader or DumpReader()
        self.converter = converter or MySQLToSQLiteConverter()

    def import_complete_groups(
        self,
        dump_path: str | Path,
        complete_groups: list[str],
        prefix: str,
        table_groups: dict = None,
    ) -> dict:
        """
        جداول گروه‌های کامل را از دامپ به دیتابیس موقت وارد می‌کند.
        برمی‌گرداند: {"tables_created": n, "inserts_count": n, "errors": [...]}
        """
        groups = table_groups or TABLE_GROUPS
        wanted_normalized = set()
        for g in complete_groups:
            if g in groups:
                wanted_normalized.update(groups[g])

        if not wanted_normalized:
            return {"tables_created": 0, "inserts_count": 0, "errors": []}

        # نگاشت: نام جدول در دامپ (با پیشوند) -> نام نرمال (بدون پیشوند)
        tables_created = set()
        inserts_count = 0
        errors = []

        with SQLiteManager(self.db_path) as db:
            db.conn.execute("PRAGMA synchronous = OFF")
            # تغییر journal_mode ممکن است در بعضی شرایط lock بدهد؛
            # در این حالت import را بدون تغییر journal_mode ادامه می‌دهیم.
            try:
                db.conn.execute("PRAGMA journal_mode = MEMORY")
            except sqlite3.OperationalError:
                pass
            db.conn.execute("BEGIN TRANSACTION")

            try:
                for stmt in self.reader.read_statements(dump_path):
                    stmt_upper = stmt.upper().strip()
                    if stmt_upper.startswith("CREATE TABLE"):
                        raw_name = self.converter._extract_table_name(stmt, "CREATE")
                        if not raw_name:
                            continue
                        target = remove_table_prefix(raw_name, prefix)
                        if target not in wanted_normalized:
                            continue
                        try:
                            converted = self.converter.convert(stmt, target).rstrip(";")
                            db.conn.execute(converted)
                            tables_created.add(target)
                        except Exception as e:
                            errors.append(f"CREATE {target}: {e}")

                    elif stmt_upper.startswith("INSERT INTO"):
                        raw_name = self.converter._extract_table_name(stmt, "INSERT")
                        if not raw_name:
                            continue
                        target = remove_table_prefix(raw_name, prefix)
                        if target not in wanted_normalized:
                            continue
                        try:
                            converted = self.converter.convert(stmt, target).rstrip(";")
                            db.conn.execute(converted)
                            inserts_count += 1
                        except Exception as e:
                            errors.append(f"INSERT {target}: {e}")

            finally:
                try:
                    db.conn.commit()
                except Exception:
                    pass

        return {
            "tables_created": len(tables_created),
            "inserts_count": inserts_count,
            "errors": errors,
        }
