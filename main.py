import sys

from bidi.algorithm import get_display

from config import DUMP_DIR, OUTPUT_DIR, RFM_FROM_SHAMSI_DATE, SQLITE_DB_PATH, TABLE_GROUPS

# رفع خطای Unicode در ویندوز
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass
from core.customer_purchases import (
    CUSTOMER_PURCHASES_VIEW,
    create_customer_purchases_view,
    get_customer_purchases_row_count,
)
from core.db_manager import SQLiteManager
from core.dump_reader import DumpReader
from core.excel_exporter import ExcelExporter
from core.importer import DumpImporter
from core.rfm_data import RFM_DATA_TABLE, create_rfm_data_table, get_rfm_data_row_count
from core.user_full_data import (
    USER_FULL_DATA_TABLE,
    create_user_full_data_table,
    get_user_full_data_row_count,
)
from utils.helpers import create_output_folder, write_output_readme


def rtl(text: str) -> str:
    """تبدیل متن فارسی/عربی برای نمایش درست در کنسول."""
    return get_display(text)


def select_dump_file() -> str | None:
    """نمایش لیست فایل‌ها و انتخاب توسط کاربر."""
    reader = DumpReader(DUMP_DIR)
    files = reader.list_files()

    if not files:
        print(rtl(f"هیچ فایل دامپی در پوشه {DUMP_DIR} یافت نشد."))
        print(rtl("فایل‌های مجاز: .sql, .gz, .sql.gz"))
        return None

    print(rtl("\nفایل‌های دامپ موجود:"))
    print("-" * 50)
    for i, f in enumerate(files):
        comp = rtl(" [فشرده]") if f["compressed"] else ""
        print(f"  {i + 1}. {f['name']} ({f['size_mb']} MB){comp}")
    print("-" * 50)

    while True:
        try:
            choice = input(rtl("شماره فایل را وارد کنید (یا Enter برای اولین فایل، 0 برای خروج):  ")).strip()
            if not choice:
                idx = 0
            else:
                idx = int(choice)
                if idx == 0:
                    return None
                idx -= 1
            path = reader.select_file(idx)
            if path:
                return str(path)
            print(rtl("شماره نامعتبر است."))
        except ValueError:
            print(rtl("لطفاً یک عدد وارد کنید."))
        except (KeyboardInterrupt, EOFError):
            print(rtl("\nلغو شد."))
            return None


def main():
    print(rtl("=== SQL to Excel Tool ==="))
    print(rtl(f"پوشه دامپ: {DUMP_DIR}"))
    print(rtl(f"خروجی: {OUTPUT_DIR}"))
    print(rtl(f"دیتابیس SQLite: {SQLITE_DB_PATH}"))
    if str(RFM_FROM_SHAMSI_DATE).strip() and str(RFM_FROM_SHAMSI_DATE).strip() != "0":
        print(rtl(f"مبنای محاسبات RFM (شمسی): {RFM_FROM_SHAMSI_DATE}"))
    else:
        print(rtl("مبنای محاسبات RFM (شمسی): از ابتدا"))

    # گام ۱: خالی کردن دیتابیس موقت
    with SQLiteManager(SQLITE_DB_PATH) as db:
        dropped = db.clear_all_tables()
    print(rtl(f"\nدیتابیس موقت خالی شد ({dropped} جدول حذف شد)."))

    dump_path = select_dump_file()
    if not dump_path:
        return

    reader = DumpReader()
    info = reader.get_info(dump_path)
    print(rtl(f"\nفایل انتخاب شده: {info['name']}"))
    print(rtl(f"حجم: {info['size_mb']} MB"))
    print(rtl(f"فشرده: {'بله' if info['compressed'] else 'خیر'}"))

    prefix = reader.detect_prefix(dump_path)
    if prefix:
        print(rtl(f"پیشوند تشخیص داده شده: '{prefix}'"))
    else:
        print(rtl("پیشوندی تشخیص داده نشد."))

    # چک کردن کدام لیست‌ها کامل جداولشان در فایل دامپ هست
    complete_groups = reader.get_complete_groups(dump_path, prefix) if TABLE_GROUPS else []
    if TABLE_GROUPS:
        print(rtl("\nبررسی لیست‌ها:"))
        for group_name in TABLE_GROUPS:
            status = "detect" if group_name in complete_groups else "not found"
            print(rtl(f"{group_name}: {status}"))

    # وارد کردن جداول گروه‌های کامل به دیتابیس موقت
    if complete_groups:
        print(rtl("\nدر حال وارد کردن جداول به دیتابیس موقت..."))
        importer = DumpImporter(SQLITE_DB_PATH)
        result = importer.import_complete_groups(dump_path, complete_groups, prefix)
        print(rtl(f"  جداول ایجاد شده: {result['tables_created']}"))
        print(rtl(f"  دستورات INSERT اجرا شده: {result['inserts_count']}"))
        if result["errors"]:
            print(rtl("  خطاها:"))
            for err in result["errors"][:5]:
                print(rtl(f"    - {err}"))
            if len(result["errors"]) > 5:
                print(rtl(f"    ... و {len(result['errors']) - 5} خطای دیگر"))

    # آمار دیتابیس موقت (برای README)
    table_row_counts = {}
    if complete_groups:
        with SQLiteManager(SQLITE_DB_PATH) as db:
            idx_result = db.ensure_recommended_indexes()
            if idx_result["created"] > 0:
                print(rtl(f"  ایندکس‌های پیشنهادی ایجاد شد ({idx_result['created']} مورد)."))
            table_row_counts = db.get_table_row_counts()

            # ساخت جدول اطلاعات خرید مشتری (فقط اگر گروه wp کامل باشد)
            if "wp" in complete_groups:
                if create_customer_purchases_view(db):
                    count = get_customer_purchases_row_count(db)
                    table_row_counts[CUSTOMER_PURCHASES_VIEW] = count
                    print(rtl(f"  جدول اطلاعات خرید مشتری ایجاد شد ({count} رکورد)."))
                else:
                    print(rtl("  خطا در ایجاد جدول اطلاعات خرید مشتری."))

                if create_user_full_data_table(db):
                    count = get_user_full_data_row_count(db)
                    table_row_counts[USER_FULL_DATA_TABLE] = count
                    print(rtl(f"  جدول user_full_data ایجاد شد ({count} رکورد)."))
                else:
                    print(rtl("  خطا در ایجاد جدول user_full_data."))

                if create_rfm_data_table(db, from_shamsi_date=RFM_FROM_SHAMSI_DATE):
                    count = get_rfm_data_row_count(db)
                    table_row_counts[RFM_DATA_TABLE] = count
                    if str(RFM_FROM_SHAMSI_DATE).strip() and str(RFM_FROM_SHAMSI_DATE).strip() != "0":
                        print(rtl(f"  جدول rfm_data ایجاد شد ({count} رکورد) - از تاریخ شمسی {RFM_FROM_SHAMSI_DATE}."))
                    else:
                        print(rtl(f"  جدول rfm_data ایجاد شد ({count} رکورد) - بدون فیلتر تاریخ."))
                else:
                    print(rtl("  خطا در ایجاد جدول rfm_data."))

    # ساخت پوشه خروجی با نام پیشوند (یا output اگر خالی بود)
    folder_name = prefix.rstrip("_") if prefix else "output"
    output_folder = create_output_folder(OUTPUT_DIR, folder_name)
    write_output_readme(
        output_folder,
        info["name"],
        info["size_mb"],
        complete_groups=complete_groups,
        table_groups=TABLE_GROUPS,
        table_row_counts=table_row_counts,
        rfm_from_shamsi_date=RFM_FROM_SHAMSI_DATE,
    )
    print(rtl(f"\nپوشه خروجی: {output_folder}"))
    print(rtl("فایل README.txt ایجاد شد."))

    # خروجی Excel جدول اطلاعات خرید مشتری (حداکثر ۵۰۰هزار ردیف در هر فایل)
    if CUSTOMER_PURCHASES_VIEW in table_row_counts:
        headers = [
            "شناسه کاربر",
            "نام کاربر",
            "ایمیل",
            "شماره موبایل",
            "شناسه سفارش",
            "تاریخ خرید",
            "مبلغ خرید",
            "وضعیت سفارش",
        ]
        with SQLiteManager(SQLITE_DB_PATH) as db:
            exporter = ExcelExporter(db, output_folder)
            paths = exporter.export_view_chunked(
                CUSTOMER_PURCHASES_VIEW,
                output_base_name="user_orders",
                column_headers=headers,
            )
            for p in paths:
                print(rtl(f"فایل Excel: {p.name}"))

    # خروجی Excel جدول user_full_data (حداکثر ۵۰۰هزار ردیف در هر فایل)
    if USER_FULL_DATA_TABLE in table_row_counts:
        with SQLiteManager(SQLITE_DB_PATH) as db:
            exporter = ExcelExporter(db, output_folder)
            paths = exporter.export_view_chunked(
                USER_FULL_DATA_TABLE,
                output_base_name="user_full_data",
            )
            for p in paths:
                print(rtl(f"فایل Excel: {p.name}"))

    # خروجی Excel جدول rfm_data (حداکثر ۵۰۰هزار ردیف در هر فایل)
    if RFM_DATA_TABLE in table_row_counts:
        with SQLiteManager(SQLITE_DB_PATH) as db:
            exporter = ExcelExporter(db, output_folder)
            paths = exporter.export_view_chunked(
                RFM_DATA_TABLE,
                output_base_name="rfm_data",
            )
            for p in paths:
                print(rtl(f"فایل Excel: {p.name}"))


if __name__ == "__main__":
    main()
