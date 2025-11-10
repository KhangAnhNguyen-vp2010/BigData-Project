import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\lenovo\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\lenovo\AppData\Local\Programs\Python\Python310\python.exe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from rethinkdb import RethinkDB
import pandas as pd
import numpy as np

r = RethinkDB()

def clean_df_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    Thay tất cả NaN, inf, -inf bằng None để JSON hợp lệ.
    """
    return df.replace([np.inf, -np.inf, np.nan], None)

try:
    # 1️⃣ Kết nối RethinkDB
    conn = r.connect('localhost', 28015)
    cursor = r.db('flights_db').table('flights').run(conn)
    df_pd = pd.DataFrame(list(cursor))
    print(f"Loaded {len(df_pd)} records from RethinkDB")

    # Kiểm tra cột timestamp
    if 'timestamp' not in df_pd.columns:
        raise ValueError("Dataset không có cột 'timestamp' để phân tích xu hướng thời gian.")

    # 2️⃣ Khởi tạo Spark
    spark = SparkSession.builder.appName("FlightDataAnalysis").getOrCreate()
    df = spark.createDataFrame(df_pd)

    # 3️⃣ Lọc dữ liệu: loại bỏ velocity không hợp lệ
    df_clean = df.filter(
        (col("velocity").isNotNull()) &
        (~col("velocity").isNaN()) &
        (~col("velocity").isin([float("inf"), float("-inf")]))
    )
    df_clean.createOrReplaceTempView("flights_clean")

    # 4️⃣ Phân tích 1: Top quốc gia có nhiều chuyến bay nhất
    result_summary = spark.sql("""
        SELECT 
            origin_country, 
            COUNT(*) AS total_flights, 
            AVG(velocity) AS avg_speed
        FROM flights_clean
        GROUP BY origin_country
        ORDER BY total_flights DESC
        LIMIT 10
    """)
    result_summary.show()

    # 5️⃣ Phân tích 2: Xu hướng chuyến bay theo thời gian (theo giờ)
    df_time_clean = df.filter(col("timestamp").isNotNull())
    df_time_clean.createOrReplaceTempView("flights_time_clean")

    result_trend = spark.sql("""
        SELECT
            date_format(from_unixtime(timestamp), 'yyyy-MM-dd HH:00:00') AS hour_slot,
            COUNT(*) AS flights_per_hour
        FROM flights_time_clean
        GROUP BY hour_slot
        ORDER BY hour_slot ASC
    """)
    result_trend.show(20)

    # 6️⃣ Lưu kết quả vào bảng summary
    results_df_summary = clean_df_for_json(result_summary.toPandas())
    table_list = r.db('flights_db').table_list().run(conn)
    if 'summary' not in table_list:
        r.db('flights_db').table_create('summary').run(conn)
        print("Created table 'summary'")

    r.db('flights_db').table('summary').delete().run(conn)
    r.db('flights_db').table('summary').insert(results_df_summary.to_dict(orient='records')).run(conn)
    print("Saved summary stats to RethinkDB")

    # 7️⃣ Lưu kết quả xu hướng
    results_df_trend = clean_df_for_json(result_trend.toPandas())
    if 'trend' not in table_list:
        r.db('flights_db').table_create('trend').run(conn)
        print("Created table 'trend'")

    r.db('flights_db').table('trend').delete().run(conn)
    r.db('flights_db').table('trend').insert(results_df_trend.to_dict(orient='records')).run(conn)
    print("Saved flight trend to RethinkDB")

    # 8️⃣ Xuất CSV
    results_df_summary.to_csv("output_flight_stats.csv", index=False)
    results_df_trend.to_csv("output_flight_trend.csv", index=False)
    print("Saved CSV files: output_flight_stats.csv, output_flight_trend.csv")

except Exception as e:
    print("Error during analysis:", e)

finally:
    # 9️⃣ Đóng kết nối & Spark
    try:
        conn.close()
        print("RethinkDB connection closed.")
    except:
        pass

    spark.stop()
    print("Flight data analysis completed!")
