# -*- coding: utf-8 -*-
"""
Execute Bills Partitioned Table Setup Script
执行账单分区表安装脚本
"""
import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
import psycopg2

load_dotenv()

# Database configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "finflow")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

def execute_sql_file(sql_file_path: str):
    """Execute SQL file on database using psycopg2"""
    print(f"Connecting to database: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    # Connect to database
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Read SQL file
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    print(f"Executing SQL file: {sql_file_path}")
    print("-" * 60)
    
    try:
        # Execute the entire SQL file at once
        cursor.execute(sql_content)
        print("✓ SQL executed successfully")
    except Exception as e:
        print(f"✗ Error: {e}")
        conn.rollback()
        return
    
    print("-" * 60)
    
    # Verify partitions
    print("\nVerifying partition tables...")
    print("=" * 60)
    
    cursor.execute("""
        SELECT 
            parent.relname AS parent_table,
            child.relname AS partition_name,
            pg_get_expr(child.relpartbound, child.oid) AS partition_expression
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
        WHERE parent.relname = 'bills'
        ORDER BY child.relname
    """)
    
    partitions = cursor.fetchall()
    if partitions:
        print(f"\n✓ Found {len(partitions)} partition tables:\n")
        for p in partitions:
            print(f"  📦 {p[1]}: {p[2]}")
    else:
        print("  ⚠ No partitions found!")
    
    # Verify community mapping
    print("\n" + "=" * 60)
    print("Verifying community mapping...")
    print("=" * 60)
    
    cursor.execute("SELECT community_id, community_name, partition_suffix FROM community_mapping ORDER BY community_id")
    mappings = cursor.fetchall()
    if mappings:
        print(f"\n✓ Found {len(mappings)} community mappings:\n")
        for m in mappings:
            print(f"  🏢 ID={m[0]}: {m[1]} (bills_proj_{m[2]})")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 60)
    print("✓ Setup complete!")
    print("=" * 60)

if __name__ == "__main__":
    sql_file = Path(__file__).parent.parent / "sql" / "setup_bills_partitions.sql"
    
    if not sql_file.exists():
        print(f"Error: SQL file not found: {sql_file}")
        sys.exit(1)
    
    execute_sql_file(str(sql_file))
