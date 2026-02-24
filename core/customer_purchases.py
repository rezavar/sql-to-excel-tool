"""
جدول اطلاعات خرید مشتری: ترکیب داده‌های users، usermeta، wc_order_stats و wc_customer_lookup.
"""
from core.db_manager import SQLiteManager


# نام view/جدول خروجی
CUSTOMER_PURCHASES_VIEW = "customer_purchases"

# کوئری برای ساخت view اطلاعات خرید مشتری
# بهینه: استفاده از JOIN به‌جای correlated subquery برای usermeta
# ستون‌ها: نام کاربر، ایمیل، شماره موبایل، شناسه سفارش، تاریخ خرید، مبلغ خرید، وضعیت سفارش
CREATE_CUSTOMER_PURCHASES_VIEW_SQL = """
DROP VIEW IF EXISTS "customer_purchases";
CREATE VIEW "customer_purchases" AS
SELECT
    u.ID AS user_id,
    u.display_name AS username,
    u.user_email AS user_email,
    pm.meta_value AS mobile,
    stats.order_id AS order_id,
    stats.date_created AS purchase_date,
    stats.total_sales AS purchase_amount,
    stats.status AS order_status
FROM wc_order_stats AS stats
JOIN wc_customer_lookup AS customers ON stats.customer_id = customers.customer_id
JOIN users AS u ON customers.user_id = u.ID
LEFT JOIN usermeta AS pm ON u.ID = pm.user_id AND pm.meta_key = 'billing_phone'
ORDER BY stats.date_created DESC;
"""


def create_customer_purchases_view(db: SQLiteManager) -> bool:
    """
    ساخت view اطلاعات خرید مشتری.
    برمی‌گرداند True اگر موفق بود، False در غیر این صورت.
    """
    try:
        db.executescript(CREATE_CUSTOMER_PURCHASES_VIEW_SQL)
        db.commit()
        return True
    except Exception:
        return False


def get_customer_purchases_row_count(db: SQLiteManager) -> int:
    """تعداد رکوردهای view اطلاعات خرید مشتری."""
    try:
        cursor = db.execute(f'SELECT COUNT(*) FROM "{CUSTOMER_PURCHASES_VIEW}"')
        return cursor.fetchone()[0]
    except Exception:
        return 0
