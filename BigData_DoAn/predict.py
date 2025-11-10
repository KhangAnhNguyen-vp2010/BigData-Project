import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\lenovo\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\lenovo\AppData\Local\Programs\Python\Python310\python.exe"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
import pandas as pd
import sys
import io

# Cho phép in Unicode ra console
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

print("🚀 Bắt đầu huấn luyện mô hình Random Forest để dự đoán số chuyến bay...")

# 1️⃣ Đọc dữ liệu
df_pd = pd.read_csv("output_flight_trend.csv")
print(f"📂 Đọc {len(df_pd)} bản ghi từ output_flight_trend.csv")

# 2️⃣ Gán chỉ số giờ liên tục
df_pd = df_pd.reset_index().rename(columns={"index": "hour_index"})

# 3️⃣ Tạo SparkSession
spark = SparkSession.builder.appName("FlightTrendPrediction_RF").getOrCreate()
df = spark.createDataFrame(df_pd)

# Đổi tên cột flights_per_hour thành label
df = df.withColumnRenamed("flights_per_hour", "label")

# 4️⃣ Tạo feature vector
assembler = VectorAssembler(inputCols=["hour_index"], outputCol="features")
data = assembler.transform(df).select("features", "label")

# 5️⃣ Huấn luyện mô hình Random Forest
rf = RandomForestRegressor(featuresCol="features", labelCol="label", numTrees=100, maxDepth=6)
model = rf.fit(data)

# 6️⃣ Dự đoán cho giờ kế tiếp
next_hour = df_pd["hour_index"].max() + 1
next_df = spark.createDataFrame([(next_hour,)], ["hour_index"])
next_df = assembler.transform(next_df)
prediction = model.transform(next_df).collect()[0]["prediction"]

# 7️⃣ Ép không âm và làm tròn
prediction = max(prediction, 0)
prediction = round(prediction, 2)

print(f"📈 Dự đoán số chuyến bay trong giờ tới: {prediction}")

spark.stop()
print("✅ Huấn luyện và dự đoán hoàn tất bằng Random Forest!")

