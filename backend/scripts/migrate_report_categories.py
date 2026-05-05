# -*- coding: utf-8 -*-
"""
Migration: Add report categories support
- Creates reporting_report_categories table
- Adds category_id column to reporting_reports
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text, inspect, Column, Integer, String, DateTime, ForeignKey, Text, Boolean
from sqlalchemy.sql import func
from database import engine, Base, SessionLocal
import models


def table_exists(conn, table_name):
    inspector = inspect(conn)
    return table_name in inspector.get_table_names()


def column_exists(conn, table_name, column_name):
    inspector = inspect(conn)
    if table_name not in inspector.get_table_names():
        return False
    columns = [col['name'] for col in inspector.get_columns(table_name)]
    return column_name in columns


def migrate():
    print("Starting report categories migration...")

    with engine.begin() as conn:
        dialect = engine.dialect.name
        print(f"Detected dialect: {dialect}")

        # 1. Create reporting_report_categories table
        if not table_exists(conn, 'reporting_report_categories'):
            print("Creating table: reporting_report_categories")
            if dialect in ('mssql', 'sqlserver'):
                conn.execute(text("""
                    CREATE TABLE reporting_report_categories (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        name NVARCHAR(100) NOT NULL,
                        parent_id INT NULL,
                        sort_order INT DEFAULT 0,
                        status INT DEFAULT 1,
                        description NVARCHAR(500) NULL,
                        created_at DATETIME DEFAULT GETDATE(),
                        updated_at DATETIME DEFAULT GETDATE(),
                        CONSTRAINT fk_report_categories_parent 
                            FOREIGN KEY (parent_id) REFERENCES reporting_report_categories(id)
                    )
                """))
            elif dialect == 'postgresql':
                conn.execute(text("""
                    CREATE TABLE reporting_report_categories (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100) NOT NULL,
                        parent_id INTEGER NULL REFERENCES reporting_report_categories(id),
                        sort_order INTEGER DEFAULT 0,
                        status INTEGER DEFAULT 1,
                        description VARCHAR(500) NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
            elif dialect == 'sqlite':
                conn.execute(text("""
                    CREATE TABLE reporting_report_categories (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name VARCHAR(100) NOT NULL,
                        parent_id INTEGER NULL REFERENCES reporting_report_categories(id),
                        sort_order INTEGER DEFAULT 0,
                        status INTEGER DEFAULT 1,
                        description VARCHAR(500) NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
            else:
                # Fallback: use SQLAlchemy create
                models.ReportingReportCategory.__table__.create(conn)
            print("Table created successfully.")
        else:
            print("Table reporting_report_categories already exists, skipping.")

        # 2. Add category_id to reporting_reports
        if not column_exists(conn, 'reporting_reports', 'category_id'):
            print("Adding column: reporting_reports.category_id")
            if dialect in ('mssql', 'sqlserver'):
                conn.execute(text("""
                    ALTER TABLE reporting_reports
                    ADD category_id INT NULL
                        CONSTRAINT fk_reporting_reports_category
                        REFERENCES reporting_report_categories(id)
                """))
            elif dialect == 'postgresql':
                conn.execute(text("""
                    ALTER TABLE reporting_reports
                    ADD COLUMN category_id INTEGER NULL
                        REFERENCES reporting_report_categories(id)
                """))
            elif dialect == 'sqlite':
                # SQLite limited ALTER TABLE support
                conn.execute(text("""
                    ALTER TABLE reporting_reports
                    ADD COLUMN category_id INTEGER NULL
                        REFERENCES reporting_report_categories(id)
                """))
            else:
                conn.execute(text("""
                    ALTER TABLE reporting_reports
                    ADD COLUMN category_id INTEGER NULL
                """))
            print("Column added successfully.")
        else:
            print("Column reporting_reports.category_id already exists, skipping.")

    print("Migration completed successfully!")


if __name__ == "__main__":
    migrate()
