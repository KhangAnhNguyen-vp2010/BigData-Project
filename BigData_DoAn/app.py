import streamlit as st
from rethinkdb import RethinkDB
r = RethinkDB()
# ========== CONFIG ==========
st.set_page_config(
    page_title="Flight Data System",
    layout="wide",
    page_icon="✈️"
)

DB_NAME = "flights_db"

# ========== HEADER ==========
st.title("✈️ Flight Data Intelligence System")
st.markdown("""
### Hệ thống giám sát và phân tích dữ liệu chuyến bay Realtime  
Dự án gồm 3 thành phần chính:
1. **📡 Realtime Flights** – hiển thị dữ liệu các chuyến bay trực tiếp từ RethinkDB.  
2. **📊 Dashboard (Spark)** – thống kê & phân tích dữ liệu chuyến bay (tốc độ, quốc gia, xu hướng, v.v).  
3. **⏰ Dự đoán** – mô hình dự đoán số chuyến bay trong giờ kế tiếp dựa trên dữ liệu lịch sử.
""")

st.divider()

# ========== STATUS CHECK ==========
col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("🗄️ RethinkDB Status")
    try:    
        # ✅ Sửa phần này: dùng rethinkdb.connect() trực tiếp, không dùng class RethinkDB
        conn = r.connect(host="localhost", port=28015)
        dbs = list(r.db_list().run(conn))
        if DB_NAME in dbs:
            st.success(f"RethinkBD đã kết nối thành công đến `{DB_NAME}` ✅")
        else:
            st.warning(f"Không tìm thấy database `{DB_NAME}`")
        conn.close()
    except Exception as e:
        st.error(f"Không thể kết nối RethinkDB: {e}")

with col2:
    st.subheader("⚡ Spark Engine")
    st.success("Người dùng có thể phân tích dữ liệu ngay bây giờ ✅")

with col3:
    st.subheader("🤖 Mô hình dự đoán")
    st.success("Người dùng có thể dự đoán mô hình ngay bây giờ ✅")

st.divider()

# ========== NAVIGATION ==========
st.markdown("## 🚀 Bắt đầu trải nghiệm")

colA, colB = st.columns(2)

with colA:
    st.image("https://cdn-icons-png.flaticon.com/512/3319/3319603.png", width=120)
    st.write("**📡 Giám sát chuyến bay (Realtime)**")
    st.write("Quan sát các chuyến bay mới nhất, vị trí và quốc gia xuất phát.")
    st.page_link("pages/1_Realtime.py", label="🔍 Mở Realtime Flights")

with colB:
    st.image("https://cdn-icons-png.flaticon.com/512/6840/6840478.png", width=120)
    st.write("**📊 Dashboard phân tích & dự đoán (Spark)**")
    st.write("Phân tích thống kê, biểu đồ, xu hướng và dự đoán số chuyến bay kế tiếp.")
    st.page_link("pages/2_Dashboard.py", label="📈 Mở Dashboard")

st.divider()

# ========== FOOTER ==========
st.caption("© 2025 Flight Data System — Built with ❤️ using Streamlit, Spark & RethinkDB")
