# -*- coding: utf-8 -*-
"""تست خواندن فایل دامپ"""
import sys
sys.stdout.reconfigure(encoding="utf-8")

from config import DUMP_DIR
from core.dump_reader import DumpReader

r = DumpReader()
files = r.list_files()
print("Files found:", len(files))
for f in files:
    print(f"  - {f['name']} ({f['size_mb']} MB) [compressed: {f['compressed']}]")

path = r.select_file(0)
if not path:
    print("No file selected")
    sys.exit(1)

info = r.get_info(path)
print()
print("Selected:", info["name"])
print("Size:", info["size_mb"], "MB")
print("Compressed:", info["compressed"])

prefix = r.detect_prefix(path)
print("Detected prefix:", repr(prefix))

count = 0
for stmt in r.read_statements(path):
    count += 1
    if count <= 3:
        first = stmt[:80].replace("\n", " ")
        print(f"  Stmt {count}:", first + "...")
print(f"Total statements: {count}")
