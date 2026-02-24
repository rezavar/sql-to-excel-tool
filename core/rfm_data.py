"""
ساخت جدول rfm_data بر اساس wc_order_stats برای تحلیل RFM.
"""
from datetime import datetime

import jdatetime

from core.db_manager import SQLiteManager


RFM_DATA_TABLE = "rfm_data"


def _to_shamsi(dt_text: str | None) -> str:
    """تبدیل datetime میلادی (YYYY-MM-DD HH:MM:SS) به تاریخ شمسی."""
    if not dt_text:
        return ""
    try:
        dt = datetime.strptime(dt_text, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""
    jdt = jdatetime.datetime.fromgregorian(datetime=dt)
    return jdt.strftime("%Y/%m/%d %H:%M:%S")


def _shamsi_to_gregorian_start(shamsi_text: str | None) -> str | None:
    """
    تبدیل تاریخ شمسی کانفیگ به datetime میلادی.
    - "0" یا خالی => None (بدون فیلتر)
    - ورودی‌های مجاز:
      YYYY/MM/DD
      YYYY-MM-DD
      YYYY/MM/DD HH:MM:SS
      YYYY-MM-DD HH:MM:SS
    """
    if shamsi_text is None:
        return None
    txt = str(shamsi_text).strip()
    if not txt or txt == "0":
        return None

    txt = txt.replace("-", "/")
    try:
        if " " in txt:
            date_part, time_part = txt.split(" ", 1)
            y, m, d = [int(x) for x in date_part.split("/")]
            hh, mm, ss = [int(x) for x in time_part.split(":")]
            jdt = jdatetime.datetime(y, m, d, hh, mm, ss)
            gdt = jdt.togregorian()
            return gdt.strftime("%Y-%m-%d %H:%M:%S")
        y, m, d = [int(x) for x in txt.split("/")]
        jdt = jdatetime.datetime(y, m, d, 0, 0, 0)
        gdt = jdt.togregorian()
        return gdt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def create_rfm_data_table(db: SQLiteManager, from_shamsi_date: str | None = None) -> bool:
    """
    ایجاد جدول rfm_data:
    - فقط سفارش‌های completed
    - آمار کل خرید هر کاربر (تعداد/مبلغ)
    - آخرین مبلغ سفارش کاربر
    - recency_days
    - فیلتر اختیاری تاریخ شروع (شمسی) از کانفیگ
    """
    try:
        db.conn.create_function("to_shamsi", 1, _to_shamsi)
        lookup_cols = db._table_columns("wc_customer_lookup")
        if "customer_id" in lookup_cols:
            join_key = "c.customer_id"
        elif "id" in lookup_cols:
            join_key = "c.id"
        else:
            return False

        from_gregorian = _shamsi_to_gregorian_start(from_shamsi_date)
        date_filter_sql = ""
        if from_gregorian:
            date_filter_sql = f" AND o.date_created >= '{from_gregorian}'"

        sql = f"""
DROP TABLE IF EXISTS "{RFM_DATA_TABLE}";
CREATE TABLE "{RFM_DATA_TABLE}" AS
WITH base AS (
    SELECT
        c.user_id AS user_id,
        o.order_id AS order_id,
        o.date_created AS date_created,
        o.total_sales AS total_sales
    FROM wc_order_stats o
    JOIN wc_customer_lookup c
        ON o.customer_id = {join_key}
    JOIN users u
        ON u.ID = c.user_id
    WHERE o.status = 'wc-completed'
    {date_filter_sql}
),
ranked AS (
    SELECT
        user_id,
        order_id,
        date_created,
        total_sales,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY date_created DESC, order_id DESC
        ) AS rn
    FROM base
),
agg AS (
    SELECT
        user_id,
        COUNT(order_id) AS total_orders,
        SUM(total_sales) AS total_spent,
        MAX(date_created) AS last_order_date
    FROM base
    GROUP BY user_id
)
SELECT
    a.user_id AS user_id,
    a.last_order_date AS last_order_date,
    to_shamsi(a.last_order_date) AS last_order_date_shamsi,
    a.total_orders AS total_orders,
    a.total_spent AS total_spent,
    r.total_sales AS last_order_amount,
    CAST(julianday('now') - julianday(a.last_order_date) AS INTEGER) AS recency_days
FROM agg a
JOIN ranked r
    ON a.user_id = r.user_id
   AND r.rn = 1;
"""
        db.executescript(sql)
        db.commit()
        return True
    except Exception:
        return False


def get_rfm_data_row_count(db: SQLiteManager) -> int:
    """تعداد رکوردهای جدول rfm_data."""
    try:
        cursor = db.execute(f'SELECT COUNT(*) FROM "{RFM_DATA_TABLE}"')
        return cursor.fetchone()[0]
    except Exception:
        return 0
