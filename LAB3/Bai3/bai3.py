import os

os.environ["PYSPARK_PYTHON"] = r"D:\Uni\junior\DS200\LAB3\venv_new\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Uni\junior\DS200\LAB3\venv_new\Scripts\python.exe"

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Lab3_Bai3").setMaster("local[*]") \
    .set("spark.python.worker.faulthandler.enabled", "true")
sc = SparkContext(conf=conf)

DELIMITER = ","

def parse_line(line):
    return line.split(DELIMITER)

def extract_user_gender(parts):
    """Trả về (UserID, Gender)"""
    return (parts[0], parts[1])  # UserID, Gender (M/F)

def extract_rating_info(parts):
    """Trả về (UserID, (MovieID, Rating))"""
    user_id = parts[0]
    movie_id = parts[1]
    rating = float(parts[2])
    return (user_id, (movie_id, rating))

def attach_gender(record):
    """
    Nhận (UserID, (MovieID, Rating))
    Trả về ((MovieID, Gender), (Rating, 1))
    """
    user_id = record[0]
    movie_id = record[1][0]
    rating = record[1][1]
    gender = gender_broadcast.value.get(user_id, "Unknown")
    return ((movie_id, gender), (rating, 1))

def sum_rating_and_count(v1, v2):
    return (v1[0] + v2[0], v1[1] + v2[1])

def calculate_average(value):
    return round(value[0] / value[1], 2)

def get_sort_key(record):
    """Sắp xếp theo MovieID rồi Gender"""
    return (record[0][0], record[0][1])

# Tạo map UserID -> Gender
users_rdd = sc.textFile("users.txt")
gender_dict = users_rdd.map(parse_line) \
                       .map(extract_user_gender) \
                       .collectAsMap()

gender_broadcast = sc.broadcast(gender_dict)

# Tạo map MovieID -> MovieTitle
movies_rdd = sc.textFile("movies.txt")
movie_title_dict = movies_rdd.map(parse_line) \
                             .map(lambda parts: (parts[0], parts[1])) \
                             .collectAsMap()

movie_title_broadcast = sc.broadcast(movie_title_dict)

# Đọc ratings, gán giới tính
ratings_rdd = sc.textFile("ratings_1.txt,ratings_2.txt")
ratings_with_gender = ratings_rdd.map(parse_line) \
                                 .map(extract_rating_info) \
                                 .map(attach_gender)

# Tính trung bình rating theo (MovieID, Gender)
avg_by_gender = ratings_with_gender.reduceByKey(sum_rating_and_count) \
                                   .mapValues(calculate_average) \
                                   .sortBy(get_sort_key)

results = avg_by_gender.collect()

print("\n" + "="*75)
print("KẾT QUẢ BÀI 3: ĐIỂM TRUNG BÌNH THEO GIỚI TÍNH")
print("="*75)
print(f"  {'Tên phim':<40} {'Gender':<10} {'Điểm TB'}")
print("-"*75)
for (movie_id, gender), avg in results:
    title = movie_title_broadcast.value.get(movie_id, f"MovieID {movie_id}")
    print(f"  {title:<40} {gender:<10} {avg:.2f} sao")
print("="*75 + "\n")

sc.stop()