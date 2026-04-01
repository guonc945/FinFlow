"""
Execute Bills Partitioned Table Setup Script for SQL Server
执行账单分区表安装脚本 - SQL Server 版本
"""
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
    print(f"正在连接到数据库: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    conn_str = (
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={DB_HOST},{DB_PORT};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD};"
        "TrustServerCertificate=yes;"
        "Encrypt=yes;"
    )
    
    conn = pyodbc.connect(conn_str)
    conn.autocommit = False
    return conn


def execute_sql_file(conn, sql_file_path: str):
    """Execute SQL file on SQL Server database"""
    print(f"正在执行 SQL 文件: {sql_file_path}")
    print("-" * 60)
    
    cursor = conn.cursor()
    
    try:
        # Read SQL file
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Split by GO statements for SQL Server
        statements = sql_content.split("GO")
        
        executed_count = 0
        for stmt in statements:
            stmt = stmt.strip()
            if not stmt or stmt.startswith("--"):
                continue
            
            try:
                cursor.execute(stmt)
                executed_count += 1
                if executed_count % 10 == 0:
                    print(f"  已执行 {executed_count} 个语句...")
            except Exception as e:
                print(f"  执行语句失败: {str(e)[:100]}")
                conn.rollback()
                raise
        
        conn.commit()
        print(f"✓ SQL 执行成功，共执行 {executed_count} 个语句")
        
    except Exception as e:
        print(f"✗ 错误: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()


def verify_partitions(conn):
    """Verify partition tables"""
    print("\n正在验证分区表...")
    print("=" * 60)
    
    cursor = conn.cursor()
    
    # Verify partition tables exist
    cursor.execute("""
        SELECT name AS table_name
        FROM sys.tables
        WHERE name LIKE 'bills_proj_%'
        ORDER BY name
    """)
    
    tables = cursor.fetchall()
    if tables:
        print(f"\n✓ 找到 {len(tables)} 个分区表:\n")
        for t in tables:
            # Get row count for each partition
            cursor.execute(f"""
                SELECT COUNT(*) FROM {t[0]}
            """)
            row_count = cursor.fetchone()[0]
            print(f"  📦 {t[0]}: {row_count} 条记录")
    else:
        print("  ⚠ 未找到分区表!")
    
    # Verify community mapping
    print("\n" + "=" * 60)
    print("验证园区映射...")
    print("=" * 60)
    
    cursor.execute("""
        SELECT community_id, community_name, partition_suffix 
        FROM community_mapping 
        ORDER BY community_id
    """)
    
    mappings = cursor.fetchall()
    if mappings:
        print(f"\n✓ 找到 {len(mappings)} 个园区映射:\n")
        for m in mappings:
            print(f"  🏢 ID={m[0]}: {m[1]} (bills_proj_{m[2]})")
    else:
        print("  ⚠ 未找到园区映射!")
    
    # Verify partition function
    print("\n" + "=" * 60)
    print("验证分区函数...")
    print("=" * 60)
    
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
        print(f"\n✓ 分区函数信息:\n")
        for f in function_info:
            print(f"  {f[0]}: 边界值 = {f[1]}")
    else:
        print("  ⚠ 未找到分区函数!")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 60)
    print("✓ 设置完成!")
    print("=" * 60)


def main():
    sql_file = Path(__file__).parent.parent / "sql" / "setup_bills_partitions_sqlserver.sql"
    
    if not sql_file.exists():
        print(f"错误: SQL 文件不存在: {sql_file}")
        sys.exit(1)
    
    try:
        conn = get_db_connection()
        execute_sql_file(conn, str(sql_file))
        verify_partitions(conn)
    except Exception as e:
        print(f"\n执行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
