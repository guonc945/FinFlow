"""创建银行账户表 kd_bank_accounts"""
from database import engine
from models import KingdeeBankAccount

KingdeeBankAccount.__table__.create(engine, checkfirst=True)
print("✅ kd_bank_accounts 表创建成功")
