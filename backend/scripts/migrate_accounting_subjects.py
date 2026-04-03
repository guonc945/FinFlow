# -*- coding: utf-8 -*-
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Setup path to backend
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import DATABASE_URL

def main():
    print(f"Connecting to database...")
    engine = create_engine(DATABASE_URL)
    
    with engine.begin() as conn:
        print("Starting migrations for accounting_subjects...")
        
        # 1. Drop column long_number
        try:
            print("Dropping column 'long_number'...")
            conn.execute(text("ALTER TABLE accounting_subjects DROP COLUMN long_number"))
            print("Dropped 'long_number'.")
        except Exception as e:
            print(f"Could not drop 'long_number' (might not exist): {e}")

        # 2. Add column ac_check
        try:
            print("Adding column 'ac_check'...")
            conn.execute(text("ALTER TABLE accounting_subjects ADD COLUMN ac_check BOOLEAN DEFAULT FALSE"))
            print("Added 'ac_check'.")
        except Exception as e:
            print(f"Could not add 'ac_check' (might exist): {e}")
            
        # 3. Add column is_qty
        try:
            print("Adding column 'is_qty'...")
            conn.execute(text("ALTER TABLE accounting_subjects ADD COLUMN is_qty BOOLEAN DEFAULT FALSE"))
            print("Added 'is_qty'.")
        except Exception as e:
            print(f"Could not add 'is_qty' (might exist): {e}")
            
        # 4. Add column currency_entry
        try:
            print("Adding column 'currency_entry'...")
            conn.execute(text("ALTER TABLE accounting_subjects ADD COLUMN currency_entry TEXT"))
            print("Added 'currency_entry'.")
        except Exception as e:
            print(f"Could not add 'currency_entry' (might exist): {e}")
            
        # conn.commit() # Not needed with engine.begin()
        print("Migration complete.")

if __name__ == "__main__":
    main()
