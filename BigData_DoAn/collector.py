import requests
from rethinkdb import RethinkDB
import time

# Kết nối RethinkDB
DB_NAME = "flights_db"
TABLE_NAME = "flights"
r = RethinkDB()
conn = r.connect(host='localhost', port=28015)
if DB_NAME not in r.db_list().run(conn):
    r.db_create(DB_NAME).run(conn)
if TABLE_NAME not in r.db(DB_NAME).table_list().run(conn):
    r.db(DB_NAME).table_create(TABLE_NAME).run(conn)

print("[INFO] Starting flight data collector...")

while True:
    try:
        # Gọi API OpenSky
        url = "https://opensky-network.org/api/states/all"
        # res = requests.get(url, timeout=10).json()
        # states = res.get("states", [])
        # timestamp = res.get("time", int(time.time()))
        res = requests.get(url, timeout=10)

        if res.status_code != 200:
            print(f"[WARN] API trả về mã lỗi {res.status_code}. Thử lại sau...")
            time.sleep(60)
            continue

        try:
            data = res.json()
        except Exception as e:
            print("[ERROR] Không thể parse JSON:", e)
            print("[DEBUG]", res.text[:300])
            time.sleep(60)
            continue

        states = data.get("states", [])
        timestamp = data.get("time", int(time.time()))


        # Chuẩn hóa dữ liệu
        docs = []
        for s in states[:80000]:  # Giới hạn 100000 bản ghi mỗi lượt cho nhẹ
            if not s:
                continue
            doc = {
                "icao24": s[0],
                "callsign": s[1] or "UNKNOWN",
                "origin_country": s[2],
                "longitude": s[5],
                "latitude": s[6],
                "velocity": s[9],
                "on_ground": s[8],
                "timestamp": timestamp
            }
            docs.append(doc)

        if docs:
            r.db(DB_NAME).table(TABLE_NAME).insert(docs).run(conn)
            print(f"[{time.strftime('%H:%M:%S')}] Inserted {len(docs)} records")

    except Exception as e:
        print("[ERROR]", e)

    # Lặp lại mỗi phút
    time.sleep(600)
