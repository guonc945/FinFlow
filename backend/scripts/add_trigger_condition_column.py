import sys
import os
from sqlalchemy import create_engine, text

# Add parent directory to path so we can import database
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import DATABASE_URL

def add_column():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        try:
            # Check if column exists (this is a bit hacky for SQLite but works generally)
            # For SQLite, we just try to add it and ignore error or check pragma
            # Using simple ALTER TABLE
            conn.execute(text("ALTER TABLE voucher_template ADD COLUMN trigger_condition TEXT"))
            conn.commit()
            print("Successfully added trigger_condition column.")
        except Exception as e:
            print(f"Error (column might already exist): {e}")

if __name__ == "__main__":
    add_column()
