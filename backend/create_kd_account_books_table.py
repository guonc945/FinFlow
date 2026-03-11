from database import engine
from models import KingdeeAccountBook
KingdeeAccountBook.__table__.create(engine, checkfirst=True)
print("KingdeeAccountBook table created!")
