# -*- coding: utf-8 -*-
"""تست انتخاب فایل از بین چند فایل"""
import sys
sys.stdout.reconfigure(encoding="utf-8")

from config import DUMP_DIR
from core.dump_reader import DumpReader

r = DumpReader()
files = r.list_files()
print("=== لیست فایل‌های دامپ ===")
print(f"تعداد: {len(files)} فایل\n")
for i, f in enumerate(files):
    comp = " [فشرده]" if f["compressed"] else ""
    print(f"  {i + 1}. {f['name']} ({f['size_mb']} MB){comp}")

print("\n" + "=" * 50)
print("تست انتخاب فایل ۱ (شماره 1):")
path1 = r.select_file(0)
print(f"  -> {path1.name if path1 else 'None'}")

print("\nتست انتخاب فایل ۲ (شماره 2):")
path2 = r.select_file(1)
print(f"  -> {path2.name if path2 else 'None'}")

print("\nتست انتخاب نامعتبر (شماره 99):")
path3 = r.select_file(98)
print(f"  -> {path3}")

print("\n" + "=" * 50)
if path1 and path2 and path1 != path2:
    print("✓ انتخاب فایل درست کار می‌کند")
else:
    print("✗ مشکلی در انتخاب وجود دارد")
