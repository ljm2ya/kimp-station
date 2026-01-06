#!/usr/bin/env python3
import sqlite3

conn = sqlite3.connect('orders.db')
cursor = conn.cursor()

print("\n📊 Database Statistics:")
print("=" * 50)

cursor.execute("SELECT source, COUNT(*) as count FROM snapshots GROUP BY source")
rows = cursor.fetchall()

for source, count in rows:
    print(f"🔹 {source}: {count} snapshots")

print("=" * 50 + "\n")

conn.close()
