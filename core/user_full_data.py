"""
ساخت جدول user_full_data از users + usermeta با ستون‌های تجمیعی.
"""
from datetime import datetime

import jdatetime

from core.db_manager import SQLiteManager


USER_FULL_DATA_TABLE = "user_full_data"


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


def _unix_to_shamsi(ts_value) -> str:
    """تبدیل unix timestamp (ثانیه) به تاریخ شمسی."""
    if ts_value is None:
        return ""
    try:
        ts_int = int(str(ts_value).strip())
        dt = datetime.utcfromtimestamp(ts_int)
    except Exception:
        return ""
    jdt = jdatetime.datetime.fromgregorian(datetime=dt)
    return jdt.strftime("%Y/%m/%d %H:%M:%S")


def create_user_full_data_table(db: SQLiteManager) -> bool:
    """
    جدول user_full_data را ایجاد می‌کند.
    - Pivot از usermeta برای meta_keyهای خواسته‌شده
    - نرمال‌سازی digits_phone
    - افزودن user_registered_timestamp و user_registered_shamsi
    """
    try:
        db.conn.create_function("to_shamsi", 1, _to_shamsi)
        db.conn.create_function("unix_to_shamsi", 1, _unix_to_shamsi)

        tables = set(db.get_tables())
        has_avans_tables = {"avans_log_score", "avans_log_refs"}.issubset(tables)

        avans_cols = ""
        if has_avans_tables:
            avans_cols = """
    ,p.avans_user_score AS avans_user_score
    ,p.avans_user_score_valid AS avans_user_score_valid
"""

        sql = f"""
DROP TABLE IF EXISTS "{USER_FULL_DATA_TABLE}";
CREATE TABLE "{USER_FULL_DATA_TABLE}" AS
WITH meta AS (
    SELECT
        user_id,
        MAX(CASE WHEN meta_key = 'nickname' THEN meta_value END) AS nickname,
        MAX(CASE WHEN meta_key = 'first_name' THEN meta_value END) AS first_name,
        MAX(CASE WHEN meta_key = 'last_name' THEN meta_value END) AS last_name,
        MAX(CASE WHEN meta_key = 'billing_first_name' THEN meta_value END) AS billing_first_name,
        MAX(CASE WHEN meta_key = 'billing_last_name' THEN meta_value END) AS billing_last_name,
        MAX(CASE WHEN meta_key = 'billing_state' THEN meta_value END) AS billing_state,
        MAX(CASE WHEN meta_key = 'billing_city' THEN meta_value END) AS billing_city,
        MAX(CASE WHEN meta_key = 'digits_phone' THEN meta_value END) AS digits_phone_raw,
        MAX(CASE WHEN meta_key = 'paying_customer' THEN meta_value END) AS paying_customer,
        MAX(CASE WHEN meta_key = 'wc_last_active' THEN meta_value END) AS wc_last_active,
        MAX(CASE WHEN meta_key = 'avans_user_score' THEN meta_value END) AS avans_user_score,
        MAX(CASE WHEN meta_key = 'avans_user_score_valid' THEN meta_value END) AS avans_user_score_valid
    FROM usermeta
    GROUP BY user_id
),
phone_norm AS (
    SELECT
        user_id,
        nickname,
        first_name,
        last_name,
        billing_first_name,
        billing_last_name,
        billing_state,
        billing_city,
        paying_customer,
        wc_last_active,
        avans_user_score,
        avans_user_score_valid,
        TRIM(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(digits_phone_raw, ''), '+', ''), ' ', ''), '-', ''), '(', ''), ')', '')
        ) AS digits_phone_clean
    FROM meta
)
SELECT
    u.ID AS ID,
    u.user_email AS user_email,
    CAST(strftime('%s', u.user_registered) AS INTEGER) AS user_registered_timestamp,
    to_shamsi(u.user_registered) AS user_registered_shamsi,
    u.display_name AS display_name,
    p.nickname AS nickname,
    p.first_name AS first_name,
    p.last_name AS last_name,
    p.billing_first_name AS illing_first_name,
    p.billing_last_name AS billing_last_name,
    p.billing_state AS billing_state,
    p.billing_city AS billing_city,
    CASE
        WHEN p.digits_phone_clean = '' THEN ''
        WHEN p.digits_phone_clean LIKE '0098%' THEN '0' || substr(p.digits_phone_clean, 5)
        WHEN p.digits_phone_clean LIKE '98%' THEN '0' || substr(p.digits_phone_clean, 3)
        WHEN p.digits_phone_clean LIKE '9%' THEN '0' || p.digits_phone_clean
        WHEN p.digits_phone_clean LIKE '0%' THEN p.digits_phone_clean
        ELSE p.digits_phone_clean
    END AS digits_phone,
    p.paying_customer AS paying_customer,
    p.wc_last_active AS wc_last_active,
    unix_to_shamsi(p.wc_last_active) AS wc_last_active_shamsi
    {avans_cols}
FROM users u
LEFT JOIN phone_norm p ON p.user_id = u.ID;
"""
        db.executescript(sql)
        db.commit()
        return True
    except Exception:
        return False


def get_user_full_data_row_count(db: SQLiteManager) -> int:
    """تعداد رکوردهای جدول user_full_data."""
    try:
        cursor = db.execute(f'SELECT COUNT(*) FROM "{USER_FULL_DATA_TABLE}"')
        return cursor.fetchone()[0]
    except Exception:
        return 0
