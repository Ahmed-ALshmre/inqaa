import re

with open("app.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if "message_type" in line and ("image" in line or "image" in line):
        print(f"Line {i+1}: {line.strip()}")
    if "HUMAN_REVIEW_ALL_IMAGES" in line:
        print(f"Line {i+1} (HUMAN): {line.strip()}")
