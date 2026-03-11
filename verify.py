import sys
import os

# Add current directory to path so we can import backend
sys.path.append(os.getcwd())

from backend.database import engine
from backend.models import Base

print("Creating tables...")
try:
    Base.metadata.create_all(bind=engine)
    print("Tables created successfully.")
except Exception as e:
    import traceback
    traceback.print_exc()
    print(f"Error: {e}")
