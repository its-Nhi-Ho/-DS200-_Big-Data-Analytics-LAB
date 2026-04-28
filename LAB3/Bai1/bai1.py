import os
import sys
import os
os.environ["PYSPARK_PYTHON"] = r"D:\Uni\junior\DS200\LAB3\venv_new\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Uni\junior\DS200\LAB3\venv_new\Scripts\python.exe"

from pyspark import SparkContext, SparkConf

# Khởi tạo cấu hình và SparkContext
conf = SparkConf().setAppName("Lab3_Bai1_NoLambda").setMaster("local[*]") \
    .set("spark.python.worker.faulthandler.enabled", "true")
sc = SparkContext(conf=conf)

DELIMITER = ","

# ==========================================
# ĐỊNH NGHĨA CÁC HÀM XỬ LÝ
# ==========================================

def parse_line(line):
    """Cắt dòng text thành một list các chuỗi dựa trên DELIMITER"""
    return line.split(DELIMITER)

def extract_movie_info(parts):
    """Đầu vào là list chứa các cột của movies.txt. Trả về (MovieID, Title)"""
    return (parts[0], parts[1])

def extract_rating_info(parts):
    """Đầu vào là list chứa các cột của ratings.txt. Trả về (MovieID, (Rating, 1))"""
    movie_id = parts[1]
    rating = float(parts[2])
    return (movie_id, (rating, 1))

def sum_rating_and_count(value1, value2):
    """Cộng dồn điểm và số lượt đánh giá trong reduceByKey"""
    # Mỗi value có dạng (Rating, Số lượt)
    sum_rating = value1[0] + value2[0]
    sum_count = value1[1] + value2[1]
    return (sum_rating, sum_count)

def calculate_average(value):
    """Tính trung bình từ (Tổng Rating, Tổng Số Lượt). Trả về (Điểm TB, Tổng Số Lượt)"""
    tong_rating = value[0]
    tong_luot = value[1]
    diem_tb = tong_rating / tong_luot
    return (diem_tb, tong_luot)

def filter_by_review_count(record):
    """Kiểm tra xem số lượt đánh giá có >= 50 hay không. Trả về True/False"""
    # record của RDD sau khi reduce và mapValues có dạng: (MovieID, (Điểm TB, Tổng Số Lượt))
    tong_luot = record[1][1]
    return tong_luot >= 5

def get_sort_key(record):
    """Chỉ định cột dùng để sắp xếp (Điểm TB)"""
    # record có dạng: (MovieID, (Điểm TB, Tổng Số Lượt))
    diem_tb = record[1][0]
    return diem_tb

# Đọc file movies.txt và tạo map (MovieID -> Title)
movies_rdd = sc.textFile("movies.txt")
movie_dict = movies_rdd.map(parse_line) \
                       .map(extract_movie_info) \
                       .collectAsMap()

# Broadcast dictionary
movie_broadcast = sc.broadcast(movie_dict)


# Đọc file ratings, map MovieID -> (Rating, 1)
ratings_rdd = sc.textFile("ratings_1.txt,ratings_2.txt")
mapped_ratings = ratings_rdd.map(parse_line) \
                            .map(extract_rating_info)


# Reduce để tính tổng điểm và số lượt đánh giá
reduced_ratings = mapped_ratings.reduceByKey(sum_rating_and_count)


# Tính điểm trung bình, lọc ra phim có ít nhất 50 lượt đánh giá
# mapValues chỉ tác động lên phần Value, giữ nguyên phần Key (MovieID)
avg_ratings = reduced_ratings.mapValues(calculate_average)
filtered_movies = avg_ratings.filter(filter_by_review_count)


# Tìm phim có điểm trung bình cao nhất
top_movie = filtered_movies.sortBy(get_sort_key, ascending=False).first()


top_movie_id = top_movie[0]
top_movie_avg = top_movie[1][0]
top_movie_count = top_movie[1][1]

# Lấy tên phim từ broadcast
top_movie_title = movie_broadcast.value.get(top_movie_id, "Không tìm thấy tên phim")

print("\n" + "="*50)
print("KẾT QUẢ BÀI 1:")
print(f"Phim có điểm cao nhất: {top_movie_title} (Mã ID: {top_movie_id})")
print(f"Điểm trung bình: {top_movie_avg:.2f} sao")
print(f"Tổng số lượt đánh giá: {top_movie_count} lượt")
print("="*50 + "\n")

sc.stop()