import psycopg2
try:
    conn = psycopg2.connect('postgresql://postgres:solo147369@localhost:5432/finflow')
    cur = conn.cursor()
    cur.execute("SELECT name, url_path, method, request_body FROM external_apis WHERE name LIKE '%辅助资料分类%'")
    rows = cur.fetchall()
    for r in rows:
        print(f'Name: {r[0]}, Path: {r[1]}, Method: {r[2]}')
        print(f'Body: {r[3]}')
    conn.close()
except Exception as e:
    print(e)
