"""Verify partition tables - Simple ASCII output for SQL Server
验证分区表 - SQL Server 版本"""
import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
import pyodbc

load_dotenv()

# Database configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "1433")
DB_NAME = os.getenv("DB_NAME", "finflow")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_DRIVER = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")


def get_db_connection():
    """Get SQL Server connection using pyodbc"""
    conn_str = (
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={DB_HOST},{DB_PORT};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD};"
        "TrustServerCertificate=yes;"
        "Encrypt=yes;"
    )
    return pyodbc.connect(conn_str)


def main():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("\n[账单分区表]")
    print("-" * 50)
    
    # Verify partition tables
    cursor.execute("""
        SELECT 
            t.name AS table_name,
            (SELECT COUNT(*) FROM sys.partitions p 
             WHERE p.object_id = t.object_id AND p.index_id IN (0, 1)) AS partition_count
        FROM sys.tables t
        WHERE t.name LIKE 'bills_proj_%'
        ORDER BY t.name
    """)
    
    tables = cursor.fetchall()
    for i, t in enumerate(tables, 1):
        # Get row count for each table
        cursor.execute(f"SELECT COUNT(*) FROM {t[0]}")
        row_count = cursor.fetchone()[0]
        print(f"  {i}. {t[0]}: {row_count} 条记录")
    
    print(f"\n总计: {len(tables)} 个分区表")
    
    print("\n[园区映射]")
    print("-" * 50)
    
    cursor.execute("""
        SELECT community_id, community_name, partition_suffix 
        FROM community_mapping 
        ORDER BY community_id
    """)
    
    mappings = cursor.fetchall()
    for m in mappings:
        print(f"  ID {m[0]} -> bills_proj_{m[2]} ({m[1]})")
    
    print(f"\n总计: {len(mappings)} 个映射")
    
    # Verify partition function
    print("\n[分区函数]")
    print("-" * 50)
    
    cursor.execute("""
        SELECT 
            pf.name AS partition_function,
            rv.value AS boundary_value
        FROM sys.partition_functions pf
        LEFT JOIN sys.partition_range_values rv ON pf.function_id = rv.function_id
        WHERE pf.name = 'pf_bills_community_id'
        ORDER BY rv.value
    """)
    
    function_info = cursor.fetchall()
    if function_info:
        print(f"\n分区函数: pf_bills_community_id")
        for f in function_info:
            print(f"  边界值: {f[1]}")
    else:
        print("  ⚠ 未找到分区函数!")
    
    cursor.close()
    conn.close()
    
    print("\n[OK] 验证完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()
