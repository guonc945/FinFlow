# -*- coding: utf-8 -*-
from sqlalchemy import create_engine, text
import os
import sys

# Setup path to backend
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import DATABASE_URL

def main():
    print(f"Connecting to database...")
    engine = create_engine(DATABASE_URL)
    
    with engine.begin() as conn:
        print("Starting migration to add long_number to accounting_subjects...")
        
        try:
            conn.execute(text("ALTER TABLE accounting_subjects ADD COLUMN long_number VARCHAR(100)"))
            print("Successfully added long_number column.")
        except Exception as e:
            print(f"Error or column already exists: {e}")
            
    print("Migration complete.")

if __name__ == "__main__":
    main()
