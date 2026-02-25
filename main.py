import sys

from bidi.algorithm import get_display

from config import DUMP_DIR, OUTPUT_DIR, RFM_FROM_SHAMSI_DATE, SQLITE_DB_PATH

# رفع خطای Unicode در ویندوز
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from flows import run_import_new_data, run_use_existing_data


def rtl(text: str) -> str:
    """تبدیل متن فارسی/عربی برای نمایش درست در کنسول."""
    return get_display(text)


def main():
    print(rtl("=== SQL to Excel Tool ==="))
    print(rtl(f"پوشه دامپ: {DUMP_DIR}"))
    print(rtl(f"خروجی: {OUTPUT_DIR}"))
    print(rtl(f"دیتابیس SQLite: {SQLITE_DB_PATH}"))
    if str(RFM_FROM_SHAMSI_DATE).strip() and str(RFM_FROM_SHAMSI_DATE).strip() != "0":
        print(rtl(f"مبنای محاسبات RFM (شمسی): {RFM_FROM_SHAMSI_DATE}"))
    else:
        print(rtl("مبنای محاسبات RFM (شمسی): از ابتدا"))

    print(rtl("\nداده‌های جدید وارد کنید (۱) یا از داده‌های وارد شده استفاده کنید (۲)؟"))
    try:
        choice = input(rtl("انتخاب (۱ یا ۲، ۰ برای خروج):  ")).strip()
    except (KeyboardInterrupt, EOFError):
        print(rtl("\nلغو شد."))
        return

    if choice == "1":
        run_import_new_data()
    elif choice == "2":
        run_use_existing_data()
    elif choice == "0":
        print(rtl("خروج."))
    else:
        print(rtl("انتخاب نامعتبر."))


if __name__ == "__main__":
    main()
