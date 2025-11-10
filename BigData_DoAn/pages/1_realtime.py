import streamlit as st
import rethinkdb as r
from rethinkdb import RethinkDB
import pandas as pd
import plotly.express as px
import os
import subprocess
# Thêm import để chạy changefeed trong thread
import threading, queue

# ================== CONFIG ==================
DB_NAME = "flights_db"
TABLE_FLIGHTS = "flights"
r = RethinkDB()

# Tạo hàng đợi và lắng nghe changefeed (singleton để chỉ khởi tạo 1 lần)
# --- Helper decorator tương thích nhiều phiên bản Streamlit ---
if hasattr(st, "cache_resource"):
    cache_singleton = st.cache_resource
else:
    # Phiên bản cũ, giữ allow_output_mutation
    def cache_singleton(func):
        return st.cache(allow_output_mutation=True)(func)

@cache_singleton
def get_changefeed_queue():
    q = queue.Queue()

    def listen():
        try:
            conn_cf = r.connect('localhost', 28015)
            feed = r.db(DB_NAME).table(TABLE_FLIGHTS).changes(include_initial=True).run(conn_cf)
            for change in feed:
                new_val = change.get("new_val")
                if new_val:
                    q.put(new_val)
        except Exception as e:
            # Ghi log nhưng không làm trang crash
            print("[Changefeed error]", e)

    threading.Thread(target=listen, daemon=True).start()
    return q

# ====== Kết nối RethinkDB (Realtime tab) ======
try:
    conn = r.connect('localhost', 28015)
    rethink_ok = True
except Exception as e:
    st.error(f"❌ Không thể kết nối RethinkDB: {e}")
    rethink_ok = False

st.set_page_config(page_title="Flight Dashboard", layout="wide")
st.sidebar.page_link("pages/2_dashboard.py", label="📊 Chuyển sang Dashboard")
st.title("✈️ Flight Data Dashboard")
if not rethink_ok:
    st.warning("⚠️ Không thể lấy dữ liệu realtime do lỗi kết nối RethinkDB.")
else:
    # Tự động refresh trang mỗi 5s để hiển thị dữ liệu mới
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=5000, key="realtime_refresh")
    except ModuleNotFoundError:
        # Nếu không có thư viện streamlit_autorefresh, bỏ qua tự refresh
        pass

    # Lấy hàng đợi changefeed (khởi tạo nếu chưa có)
    q = get_changefeed_queue()

    # Duy trì dữ liệu realtime trong session_state
    if "realtime_records" not in st.session_state:
            st.session_state["realtime_records"] = []

    # Rút dữ liệu mới nếu có
    while not q.empty():
        st.session_state["realtime_records"].append(q.get())
        # Giữ tối đa 200 bản ghi gần nhất
        st.session_state["realtime_records"] = st.session_state["realtime_records"]

    if not st.session_state["realtime_records"]:
        st.warning("Chưa có dữ liệu, chờ collector chạy...")
    else:
        df = pd.DataFrame(st.session_state["realtime_records"])
        top_countries = df["origin_country"].value_counts()
        st.subheader("Dữ liệu các chuyến bay mới nhất")
        st.bar_chart(top_countries)

        st.subheader("Bản đồ vị trí máy bay (Realtime)")
        map_df = df.dropna(subset=["latitude", "longitude"])
        st.map(map_df[["latitude", "longitude"]])
    