import streamlit as st
import pandas as pd
import plotly.express as px
import subprocess

st.set_page_config(page_title="Analysis-Predict", layout="wide")
st.sidebar.page_link("pages/1_realtime.py", label="📡 Chuyển sang Realtime Flights")
st.title("✈️ Flight Data Dashboard")

tab1, tab2 = st.tabs(["📊 Phân tích (Spark)", "🔮 Dự đoán"])

# =============== TAB 1: SPARK ANALYSIS ===============
with tab1:
    st.subheader("📊 Phân tích dữ liệu với Spark")

    summary_path = "output_flight_stats.csv"
    trend_path = "output_flight_trend.csv"

    # --- Nút chạy lại analysis.py ---
    if st.button("🔁 Phân tích lại dữ liệu (chạy analysis.py)"):
        st.info("🚀 Đang phân tích dữ liệu mới... vui lòng chờ...")
        log_placeholder = st.empty()

        import subprocess, sys

        process = subprocess.Popen(
            [sys.executable, "analysis.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )

        log_lines = []
        for line in process.stdout:
            log_lines.append(line.strip())
            # Hiển thị log trực tiếp trên web
            log_placeholder.markdown(
    f"<div style='height:300px; overflow-y:scroll; background-color:#000; color:#0f0; padding:10px; font-family:monospace; border-radius:6px;'>"
    + "<br>".join(log_lines) +
    "</div>",
    unsafe_allow_html=True
)

        process.wait()
        if process.returncode == 0:
            st.success("✅ Phân tích hoàn tất! Dữ liệu mới đã được cập nhật.")
        else:
            st.error("❌ Phân tích thất bại. Kiểm tra log ở trên hoặc console.")

    st.divider()

    # --- Đọc lại dữ liệu từ file CSV ---
    try:
        df_summary = pd.read_csv("output_flight_stats.csv")
        st.subheader("📊 Top 10 quốc gia có nhiều chuyến bay nhất (Spark)")
        st.dataframe(df_summary)

        fig = px.bar(
            df_summary,
            x="origin_country",
            y="total_flights",
            color="avg_speed",
            title="Top 10 quốc gia có nhiều chuyến bay nhất (Spark)"
        )
        st.plotly_chart(fig, use_container_width=True, key="summary_chart")

        fig2 = px.line(
            df_summary,
            x="origin_country",
            y="avg_speed",
            markers=True,
            title="Vận tốc trung bình của các quốc gia"
        )
        st.plotly_chart(fig2, use_container_width=True)
    except Exception as e:
        st.warning(f"⚠️ Không thể đọc file output_flight_stats.csv: {e}")

    st.divider()

    # --- Biểu đồ xu hướng ---
    try:
        df_trend = pd.read_csv("output_flight_trend.csv")
        if "hour_slot" in df_trend.columns:
            df_trend["hour_slot"] = pd.to_datetime(df_trend["hour_slot"])
            fig_trend = px.line(
                df_trend,
                x="hour_slot",
                y="flights_per_hour",
                title="📈 Xu hướng số chuyến bay theo thời gian (theo giờ)",
                markers=True
            )
            st.plotly_chart(fig_trend, use_container_width=True,  key="trend_chart")
    except Exception as e:
        st.warning(f"⚠️ Không thể đọc file output_flight_trend.csv: {e}")

# =============== TAB 2: DỰ ĐOÁN ===============
with tab2:
    st.subheader("🔮 Dự đoán số chuyến bay trong giờ tới")

    st.write("Hệ thống sẽ dùng dữ liệu từ `output_flight_trend.csv` để huấn luyện mô hình dự đoán và ước lượng số chuyến bay trong giờ kế tiếp.")

    if st.button("⏰ Dự đoán giờ kế tiếp"):
        import pandas as pd
        from sklearn.linear_model import LinearRegression
        import numpy as np

        try:
            df = pd.read_csv("output_flight_trend.csv")

            if "flights_per_hour" not in df.columns:
                st.error("❌ File `output_flight_trend.csv` không có cột `flights_per_hour`!")
            else:
                # Chuẩn bị dữ liệu
                df = df.reset_index().rename(columns={"index": "hour_index"})
                X = np.array(df["hour_index"]).reshape(-1, 1)
                y = np.array(df["flights_per_hour"])

                # Huấn luyện mô hình hồi quy tuyến tính
                model = LinearRegression()
                model.fit(X, y)

                # Dự đoán cho giờ kế tiếp
                next_hour = np.array([[len(df)]])
                predicted = model.predict(next_hour)[0]

                st.success(f"⏰ Dự đoán số chuyến bay trong giờ kế tiếp: **{predicted:.0f} chuyến**")

                # Hiển thị biểu đồ xu hướng + điểm dự đoán
                import plotly.graph_objects as go
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=df["hour_index"], y=df["flights_per_hour"],
                                         mode='lines+markers', name='Thực tế'))
                fig.add_trace(go.Scatter(x=[len(df)], y=[predicted],
                                         mode='markers', name='Dự đoán',
                                         marker=dict(size=12, color='red')))
                fig.update_layout(title="📈 Xu hướng và dự đoán chuyến bay theo giờ",
                                  xaxis_title="Giờ (index)",
                                  yaxis_title="Số chuyến bay")
                st.plotly_chart(fig, use_container_width=True)

        except FileNotFoundError:
            st.error("❌ Không tìm thấy file `output_flight_trend.csv`. Hãy chạy `analysis.py` trước.")
        except Exception as e:
            st.error(f"⚠️ Lỗi khi dự đoán: {e}")


