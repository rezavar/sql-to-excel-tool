import sqlite3
from pathlib import Path


class SQLiteManager:
    """Manages SQLite database operations."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.conn = None

    def connect(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        return self

    def execute(self, sql: str, params=None):
        if params:
            return self.conn.execute(sql, params)
        return self.conn.execute(sql)

    def executescript(self, sql: str):
        self.conn.executescript(sql)

    def commit(self):
        self.conn.commit()

    def get_tables(self) -> list[str]:
        cursor = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        return [row[0] for row in cursor.fetchall()]

    def get_table_row_counts(self) -> dict[str, int]:
        """برمی‌گرداند: {"table_name": row_count} برای جداول کاربری."""
        result = {}
        cursor = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        for (name,) in cursor.fetchall():
            count = self.conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
            result[name] = count
        return result

    def _table_exists(self, table_name: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table_name,),
        ).fetchone()
        return row is not None

    def _table_columns(self, table_name: str) -> set[str]:
        if not self._table_exists(table_name):
            return set()
        cursor = self.conn.execute(f'PRAGMA table_info("{table_name}")')
        return {row[1] for row in cursor.fetchall()}  # row[1] = column name

    def _index_exists(self, index_name: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name=? LIMIT 1",
            (index_name,),
        ).fetchone()
        return row is not None

    def _create_index_if_possible(self, index_name: str, table_name: str, columns: list[str]) -> bool:
        """
        اگر جدول/ستون‌ها وجود داشته باشند و ایندکس از قبل نباشد، ایندکس ساخته می‌شود.
        خروجی: True اگر ایندکس جدید ساخته شد.
        """
        if self._index_exists(index_name):
            return False
        table_cols = self._table_columns(table_name)
        if not table_cols:
            return False
        if not all(c in table_cols for c in columns):
            return False

        cols_sql = ", ".join(f'"{c}"' for c in columns)
        self.conn.execute(f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{table_name}" ({cols_sql})')
        return True

    def ensure_recommended_indexes(self) -> dict[str, int]:
        """
        ایجاد ایندکس‌های پیشنهادی برای سرعت JOIN/Filter در جداول اصلی.
        این متد فقط ایندکس‌هایی را می‌سازد که جدول/ستونشان وجود داشته باشد.
        """
        created = 0

        # wc_order_stats
        if self._create_index_if_possible("idx_wc_order_stats_customer_id", "wc_order_stats", ["customer_id"]):
            created += 1
        if self._create_index_if_possible("idx_wc_order_stats_date_created", "wc_order_stats", ["date_created"]):
            created += 1
        if self._create_index_if_possible("idx_wc_order_stats_status", "wc_order_stats", ["status"]):
            created += 1

        # wc_customer_lookup (بعضی دامپ‌ها customer_id دارند، بعضی id)
        lookup_cols = self._table_columns("wc_customer_lookup")
        if "customer_id" in lookup_cols:
            if self._create_index_if_possible("idx_wc_customer_lookup_customer_id", "wc_customer_lookup", ["customer_id"]):
                created += 1
        elif "id" in lookup_cols:
            if self._create_index_if_possible("idx_wc_customer_lookup_id", "wc_customer_lookup", ["id"]):
                created += 1
        if self._create_index_if_possible("idx_wc_customer_lookup_user_id", "wc_customer_lookup", ["user_id"]):
            created += 1

        # usermeta
        if self._create_index_if_possible("idx_usermeta_user_id", "usermeta", ["user_id"]):
            created += 1
        if self._create_index_if_possible("idx_usermeta_meta_key", "usermeta", ["meta_key"]):
            created += 1
        if self._create_index_if_possible("idx_usermeta_user_id_meta_key", "usermeta", ["user_id", "meta_key"]):
            created += 1

        self.commit()
        return {"created": created}

    def clear_all_tables(self) -> int:
        """حذف همه جداول و داده‌ها. فقط جداول کاربری (نه sqlite_*). برمی‌گرداند تعداد جداول حذف‌شده."""
        cursor = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        tables = [row[0] for row in cursor.fetchall()]
        for t in tables:
            self.conn.execute(f'DROP TABLE IF EXISTS "{t}"')
        self.commit()
        return len(tables)

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit()
        self.close()
