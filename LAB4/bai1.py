from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os

spark = SparkSession.builder \
    .appName("Fecom Analysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark version:", spark.version)

# Thư mục lưu output
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def save_csv(df, name):
    """Lưu DataFrame ra 1 file CSV duy nhất trong thư mục output/"""
    path = os.path.join(OUTPUT_DIR, name)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)
    print(f"  >> Da luu: {path}/")

# Câu 1: Đọc dữ liệu CSV (delimiter = ";", tự suy ra kiểu dữ liệu)

orders      = spark.read.csv("orders.csv",        header=True, inferSchema=True, sep=";")
customers   = spark.read.csv("Customer_List.csv", header=True, inferSchema=True, sep=";")
order_items = spark.read.csv("order_items.csv",   header=True, inferSchema=True, sep=";")
products    = spark.read.csv("products.csv",       header=True, inferSchema=True, sep=";")
reviews     = spark.read.csv("order_reviews.csv", header=True, inferSchema=True, sep=";")

print("\n=== Orders ===")
orders.printSchema()
print("So dong:", orders.count())

print("\n=== Customers ===")
customers.printSchema()
print("So dong:", customers.count())

print("\n=== Order Items ===")
order_items.printSchema()
print("So dong:", order_items.count())

print("\n=== Products ===")
products.printSchema()
print("So dong:", products.count())

print("\n=== Reviews ===")
reviews.printSchema()
print("So dong:", reviews.count())

# Câu 2: Thống kê tổng số đơn hàng, khách hàng, người bán

total_orders    = orders.count()
total_customers = customers.select("Customer_Trx_ID").distinct().count()
total_sellers   = order_items.select("Seller_ID").distinct().count()

print(f"Tong so don hang      : {total_orders:,}")
print(f"So khach hang duy nhat: {total_customers:,}")
print(f"So nguoi ban duy nhat : {total_sellers:,}")

summary_df = spark.createDataFrame([
    ("Tong so don hang",       total_orders),
    ("So khach hang duy nhat", total_customers),
    ("So nguoi ban duy nhat",  total_sellers),
], ["Metric", "Value"])
save_csv(summary_df, "cau2_tong_quan")

# Câu 3: Số lượng đơn hàng theo quốc gia (giảm dần)

orders_by_country = (
    orders
    .join(customers,
          orders["Customer_Trx_ID"] == customers["Customer_Trx_ID"],
          "left")
    .groupBy("Customer_Country")
    .agg(F.count("*").alias("Total_Orders"))
    .orderBy(F.desc("Total_Orders"))
)

orders_by_country.show(30, truncate=False)
save_csv(orders_by_country, "cau3_don_hang_theo_quoc_gia")

# Câu 4: Số lượng đơn hàng theo năm (tăng dần) và tháng (giảm dần)

orders_by_year_month = (
    orders
    .withColumn("Year",  F.year(F.col("Order_Purchase_Timestamp")))
    .withColumn("Month", F.month(F.col("Order_Purchase_Timestamp")))
    .groupBy("Year", "Month")
    .agg(F.count("*").alias("Total_Orders"))
    .orderBy(F.asc("Year"), F.desc("Month"))
)

orders_by_year_month.show(50)
save_csv(orders_by_year_month, "cau4_don_hang_theo_nam_thang")

# Câu 5: Thống kê điểm đánh giá (xử lý NULL và ngoại lệ)

reviews_clean = (
    reviews
    .withColumn("Review_Score", F.expr("try_cast(Review_Score as int)"))
    .filter(F.col("Review_Score").isNotNull())
    .filter((F.col("Review_Score") >= 1) & (F.col("Review_Score") <= 5))
)

avg_score = reviews_clean.agg(
    F.round(F.avg("Review_Score"), 2).alias("Avg_Score")
).collect()[0]["Avg_Score"]

print(f"Diem danh gia trung binh: {avg_score}")
print("\nPhan phoi theo tung muc:")

score_dist = (
    reviews_clean
    .groupBy("Review_Score")
    .agg(F.count("*").alias("Count"))
    .orderBy("Review_Score")
)
score_dist.show()
save_csv(score_dist, "cau5_phan_phoi_diem_danh_gia")

# Câu 6: Doanh thu 2024 theo danh mục sản phẩm

orders_2024 = (
    orders
    .withColumn("Year", F.year(F.col("Order_Purchase_Timestamp")))
    .filter(F.col("Year") == 2024)
    .select("Order_ID")
)

revenue_2024 = (
    orders_2024
    .join(order_items, "Order_ID", "inner")
    .join(products, "Product_ID", "inner")
    .withColumn("Revenue", F.col("Price") + F.col("Freight_Value"))
    .groupBy("Product_Category_Name")
    .agg(
        F.round(F.sum("Revenue"), 2).alias("Total_Revenue"),
        F.count("Order_ID").alias("Total_Orders")
    )
    .orderBy(F.desc("Total_Revenue"))
)

revenue_2024.show(30, truncate=False)
save_csv(revenue_2024, "cau6_doanh_thu_2024_theo_danh_muc")


# Câu 8: Hiệu suất giao hàng

delivery_perf = (
    orders
    .filter(F.col("Order_Delivered_Carrier_Date").isNotNull())
    .join(order_items, "Order_ID", "inner")
    .filter(F.col("Shipping_Limit_Date").isNotNull())
    .withColumn(
        "Delivery_Diff_Days",
        F.datediff(
            F.col("Order_Delivered_Carrier_Date").cast("date"),
            F.col("Shipping_Limit_Date").cast("date")
        )
    )
)

print("Thong ke tong quan:")
delivery_perf.agg(
    F.count("Order_ID").alias("Total_Orders"),
    F.round(F.avg("Delivery_Diff_Days"), 2).alias("Avg_Diff_Days"),
    F.min("Delivery_Diff_Days").alias("Min_Diff_Days"),
    F.max("Delivery_Diff_Days").alias("Max_Diff_Days"),
    F.sum(F.when(F.col("Delivery_Diff_Days") <= 0, 1).otherwise(0)).alias("On_Time_Or_Early"),
    F.sum(F.when(F.col("Delivery_Diff_Days") > 0, 1).otherwise(0)).alias("Late_Deliveries")
).show()

print("Phan loai trang thai giao hang:")
delivery_classified = (
    delivery_perf
    .withColumn(
        "Status",
        F.when(F.col("Delivery_Diff_Days") < 0, "Giao som")
         .when(F.col("Delivery_Diff_Days") == 0, "Dung han")
         .otherwise("Giao tre")
    )
    .groupBy("Status")
    .agg(
        F.count("*").alias("Count"),
        F.round(F.avg("Delivery_Diff_Days"), 2).alias("Avg_Diff_Days")
    )
    .orderBy("Status")
)
delivery_classified.show()
save_csv(delivery_classified, "cau8_hieu_suat_giao_hang")

# Câu 10: Xếp hạng seller theo doanh thu và số đơn hàng

seller_stats = (
    order_items
    .withColumn("Revenue", F.col("Price") + F.col("Freight_Value"))
    .groupBy("Seller_ID")
    .agg(
        F.round(F.sum("Revenue"), 2).alias("Total_Revenue"),
        F.countDistinct("Order_ID").alias("Total_Orders")
    )
)

window_revenue = Window.orderBy(F.desc("Total_Revenue"))
window_orders  = Window.orderBy(F.desc("Total_Orders"))

seller_ranked = (
    seller_stats
    .withColumn("Revenue_Rank", F.dense_rank().over(window_revenue))
    .withColumn("Orders_Rank",  F.dense_rank().over(window_orders))
    .orderBy("Revenue_Rank")
)

print("Top 20 Seller theo Doanh thu:")
seller_ranked.show(20, truncate=False)
save_csv(seller_ranked, "cau10_xep_hang_seller")

print("\n" + "="*60)
print(f"Hoan thanh! Cac file CSV duoc luu trong thu muc: ./{OUTPUT_DIR}/")
print("="*60)

spark.stop()