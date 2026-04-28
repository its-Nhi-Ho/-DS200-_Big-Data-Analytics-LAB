import os
from datetime import datetime

os.environ["PYSPARK_PYTHON"] = r"D:\Uni\junior\DS200\LAB3\venv_new\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Uni\junior\DS200\LAB3\venv_new\Scripts\python.exe"

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Lab3_Bai6").setMaster("local[*]") \
    .set("spark.python.worker.faulthandler.enabled", "true")
sc = SparkContext(conf=conf)

DELIMITER = ","


def parse_line(line):
    return line.split(DELIMITER)

def extract_year_rating(parts):
    """
    Chuyển Unix timestamp thành năm.
    Trả về (Year, (Rating, 1))
    """
    rating = float(parts[2])
    timestamp = int(parts[3])
    year = datetime.utcfromtimestamp(timestamp).year
    return (year, (rating, 1))

def sum_rating_and_count(v1, v2):
    return (v1[0] + v2[0], v1[1] + v2[1])

def calculate_avg_and_count(value):
    avg = round(value[0] / value[1], 2)
    return (avg, value[1])

def get_sort_key(record):
    """Sắp xếp theo năm tăng dần"""
    return record[0]

ratings_rdd = sc.textFile("ratings_1.txt,ratings_2.txt")

avg_by_year = ratings_rdd.map(parse_line) \
                         .map(extract_year_rating) \
                         .reduceByKey(sum_rating_and_count) \
                         .mapValues(calculate_avg_and_count) \
                         .sortBy(get_sort_key)

results = avg_by_year.collect()

print("\n" + "="*55)
print("KẾT QUẢ BÀI 6: ĐIỂM TRUNG BÌNH THEO NĂM")
print("="*55)
print(f"  {'Năm':<10} {'Điểm TB':<12} {'Tổng lượt đánh giá'}")
print("-"*55)
for year, (avg, count) in results:
    print(f"  {year:<10} {avg:<12.2f} {count} lượt")
print("="*55 + "\n")

sc.stop()