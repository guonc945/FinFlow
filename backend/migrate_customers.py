from database import engine
from models import Customer

if __name__ == "__main__":
    Customer.__table__.create(engine, checkfirst=True)
    print("Customers table created successfully.")
