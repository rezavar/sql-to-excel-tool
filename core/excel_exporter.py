from pathlib import Path

import xlsxwriter
from openpyxl import Workbook

from config import EXCEL_MAX_ROWS_PER_FILE
from core.db_manager import SQLiteManager


def _ensure_str(val):
    """تبدیل مقدار به رشته با پشتیبانی صحیح از Unicode/فارسی."""
    if val is None:
        return ""
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    return str(val)


class ExcelExporter:
    """Exports SQLite tables to Excel files."""

    def __init__(self, db_manager: SQLiteManager, output_dir: str | Path):
        self.db = db_manager
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export_table(self, table_name: str, column_headers: list[str] | None = None) -> Path:
        cursor = self.db.execute(f'SELECT * FROM "{table_name}"')
        columns = column_headers or [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        wb = Workbook()
        ws = wb.active
        ws.title = table_name[:31]
        ws.append(columns)
        for row in rows:
            ws.append(list(row))

        output_path = self.output_dir / f"{table_name}.xlsx"
        wb.save(str(output_path))
        return output_path

    def export_all(self) -> list[Path]:
        tables = self.db.get_tables()
        exported = []
        for table in tables:
            path = self.export_table(table)
            exported.append(path)
            print(f"  Exported: {table} -> {path.name}")
        return exported

    def export_view_chunked(
        self,
        view_name: str,
        output_base_name: str,
        column_headers: list[str] | None = None,
        max_rows_per_file: int | None = None,
        column_formats: dict[str, str] | None = None,
    ) -> list[Path]:
        """
        خروجی view به چند فایل Excel با حداکثر ردیف مشخص.
        نام فایل‌ها: 1_{base}.xlsx, 2_{base}.xlsx, ...
        از xlsxwriter برای پشتیبانی صحیح از Unicode و متن فارسی استفاده می‌شود.
        column_formats: نام ستون -> رشته فرمت عددی xlsxwriter (مثلاً "#,##0.00" برای کاما استایل).
        """
        max_rows = max_rows_per_file or EXCEL_MAX_ROWS_PER_FILE
        total = self.db.execute(f'SELECT COUNT(*) FROM "{view_name}"').fetchone()[0]
        if total == 0:
            return []

        exported = []
        file_index = 1
        offset = 0

        # فرمت RTL برای نمایش صحیح متن فارسی
        rtl_format = None

        while offset < total:
            limit = min(max_rows, total - offset)
            cursor = self.db.execute(
                f'SELECT * FROM "{view_name}" LIMIT ? OFFSET ?',
                (limit, offset),
            )
            columns = column_headers or [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            filename = f"{file_index}_{output_base_name}.xlsx"
            output_path = self.output_dir / filename

            wb = xlsxwriter.Workbook(
                str(output_path),
                options={"strings_to_urls": False, "constant_memory": True},
            )
            ws = wb.add_worksheet(output_base_name[:31])
            ws.right_to_left()  # جهت راست‌به‌چپ برای فارسی

            if rtl_format is None:
                rtl_format = wb.add_format({"reading_order": 2})  # RTL

            # فرمت‌های عددی اختیاری (کاما استایل و غیره) با RTL
            format_cache: dict[str, object] = {}
            if column_formats:
                for fmt_str in column_formats.values():
                    if fmt_str not in format_cache:
                        format_cache[fmt_str] = wb.add_format(
                            {"reading_order": 2, "num_format": fmt_str}
                        )

            # هدر
            for col, val in enumerate(columns):
                ws.write(0, col, _ensure_str(val), rtl_format)

            # داده‌ها
            for row_idx, row in enumerate(rows):
                for col_idx, val in enumerate(row):
                    if val is None:
                        cell_val = ""
                        fmt = rtl_format
                    elif isinstance(val, (int, float)):
                        cell_val = val
                        col_name = columns[col_idx] if col_idx < len(columns) else None
                        if (
                            column_formats
                            and col_name
                            and col_name in column_formats
                        ):
                            fmt = format_cache[column_formats[col_name]]
                        else:
                            fmt = rtl_format
                    else:
                        cell_val = _ensure_str(val)
                        fmt = rtl_format
                    ws.write(row_idx + 1, col_idx, cell_val, fmt)

            wb.close()
            exported.append(output_path)

            offset += limit
            file_index += 1

        return exported
