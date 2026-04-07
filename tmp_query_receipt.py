import pyodbc
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=fn.hyqy.group,1433;DATABASE=finflow;UID=sa;PWD=sa@sqlserver;Encrypt=yes;TrustServerCertificate=yes', timeout=10)
cur = conn.cursor()
print('receipt_bills')
cur.execute("SELECT TOP 10 id, community_id, receipt_id, income_amount, amount, deal_type, asset_name FROM receipt_bills WHERE receipt_id='1095600000547' ORDER BY id DESC")
for row in cur.fetchall():
    print(tuple(row))
print('bills')
cur.execute("SELECT id, community_id, charge_item_name, amount, deal_log_id, receipt_id, asset_name FROM bills WHERE deal_log_id IN (SELECT id FROM receipt_bills WHERE receipt_id='1095600000547') ORDER BY id")
for row in cur.fetchall():
    print(tuple(row))