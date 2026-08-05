#!/usr/bin/env python3
"""สร้างไอคอน PWA — ตัว T เซริฟในกรอบเส้นคาด อย่างหัวหนังสือพิมพ์

เขียน PNG เองด้วย zlib ล้วนๆ จะได้ไม่ต้องพึ่ง Pillow บน GitHub Actions
รัน:  python make_icons.py
"""

import struct
import zlib

PAPER = (0x0A, 0x0E, 0x1A)      # พื้นเข้มแบบธีมเว็บ
INK = (0xF4, 0xEF, 0xE3)        # ตัวอักษรสีครีม

# สัดส่วนแบบ 0–1 ของด้านกว้าง (x0, y0, x1, y1) — วางไว้ในโซนปลอดภัยของ maskable icon
SHAPES = [
    (0.26, 0.238, 0.74, 0.254),      # เส้นคาดบน
    (0.28, 0.330, 0.72, 0.400),      # หัวตัว T
    (0.28, 0.400, 0.313, 0.436),     # เชิงหัวซ้าย
    (0.687, 0.400, 0.72, 0.436),     # เชิงหัวขวา
    (0.455, 0.330, 0.545, 0.660),    # ขาตัว T
    (0.380, 0.660, 0.620, 0.702),    # เชิงล่าง
    (0.26, 0.746, 0.74, 0.762),      # เส้นคาดล่าง
]


def render(size):
    rows = []
    boxes = [(int(a * size), int(b * size), int(c * size), int(d * size))
             for a, b, c, d in SHAPES]
    for y in range(size):
        row = bytearray()
        spans = [(x0, x1) for x0, y0, x1, y1 in boxes if y0 <= y < y1]
        for x in range(size):
            row += bytes(INK if any(x0 <= x < x1 for x0, x1 in spans) else PAPER)
        rows.append(row)
    return rows


def write_png(path, size):
    raw = b"".join(b"\x00" + bytes(r) for r in render(size))

    def chunk(tag, data):
        return (struct.pack(">I", len(data)) + tag + data
                + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF))

    png = (b"\x89PNG\r\n\x1a\n"
           + chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0))
           + chunk(b"IDAT", zlib.compress(raw, 9))
           + chunk(b"IEND", b""))
    with open(path, "wb") as f:
        f.write(png)
    print(f"  ✓ {path} ({size}x{size}, {len(png):,} bytes)")


if __name__ == "__main__":
    write_png("icon-192.png", 192)
    write_png("icon-512.png", 512)
    write_png("apple-touch-icon.png", 180)
