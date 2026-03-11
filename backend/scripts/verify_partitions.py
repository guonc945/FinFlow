"""Verify partition tables - Simple ASCII output"""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST', 'localhost'),
    port=os.getenv('DB_PORT', '5432'),
    database=os.getenv('DB_NAME', 'finflow'),
    user=os.getenv('DB_USER', 'postgres'),
    password=os.getenv('DB_PASSWORD', '')
)
cur = conn.cursor()

print("\n[Bills Partition Tables]")
print("-" * 50)

cur.execute("""
    SELECT child.relname AS partition
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
    WHERE parent.relname = 'bills'
    ORDER BY child.relname
""")

rows = cur.fetchall()
for i, r in enumerate(rows, 1):
    print(f"  {i}. {r[0]}")

print(f"\nTotal: {len(rows)} partitions")

print("\n[Community Mappings]")
print("-" * 50)

cur.execute("SELECT community_id, partition_suffix FROM community_mapping ORDER BY community_id")
mappings = cur.fetchall()
for m in mappings:
    print(f"  ID {m[0]} -> bills_proj_{m[1]}")

print(f"\nTotal: {len(mappings)} mappings")

cur.close()
conn.close()
print("\n[OK] Verification complete!")
