import os

os.environ["PYSPARK_PYTHON"] = r"D:\Uni\junior\DS200\LAB3\venv_new\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Uni\junior\DS200\LAB3\venv_new\Scripts\python.exe"

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Lab3_Bai4").setMaster("local[*]") \
    .set("spark.python.worker.faulthandler.enabled", "true")
sc = SparkContext(conf=conf)

DELIMITER = ","

def parse_line(line):
    return line.split(DELIMITER)

def get_age_group(age):
    """Phân loại tuổi thành nhóm"""
    age = int(age)
    if age < 18:
        return "0 - 18"
    elif age < 35:
        return "18 - 35"
    elif age < 50:
        return "35 -50"
    else:
        return "50+"

def extract_user_agegroup(parts):
    """Trả về (UserID, AgeGroup)"""
    user_id = parts[0]
    age_group = get_age_group(parts[2])
    return (user_id, age_group)

def extract_rating_info(parts):
    return (parts[0].strip(), (parts[1].strip(), float(parts[2])))

def attach_age_group(record):
    """
    Nhận (UserID, (MovieID, Rating))
    Trả về ((MovieID, AgeGroup), (Rating, 1))
    """
    user_id = record[0]
    movie_id = record[1][0]
    rating = record[1][1]
    age_group = age_group_broadcast.value.get(user_id, "Unknown")
    return ((movie_id, age_group), (rating, 1))

def sum_rating_and_count(v1, v2):
    return (v1[0] + v2[0], v1[1] + v2[1])

def calculate_average(value):
    return round(value[0] / value[1], 2)

def get_sort_key(record):
    return (record[0][0], record[0][1])

# Tạo map UserID -> AgeGroup
users_rdd = sc.textFile("users.txt")
age_group_dict = users_rdd.map(parse_line) \
    .filter(lambda x: len(x) >= 3) \
    .map(extract_user_agegroup) \
    .collectAsMap()

age_group_broadcast = sc.broadcast(age_group_dict)

# Tạo map MovieID -> MovieTitle
movies_rdd = sc.textFile("movies.txt")

movie_title_dict = movies_rdd.map(parse_line) \
    .map(lambda parts: (parts[0].strip(), parts[1].strip())) \
    .collectAsMap()
movie_title_broadcast = sc.broadcast(movie_title_dict)

# Đọc ratings, gán nhóm tuổi
ratings_rdd = sc.textFile("ratings_1.txt,ratings_2.txt")
ratings_with_age = ratings_rdd.map(parse_line) \
                              .map(extract_rating_info) \
                              .map(attach_age_group)

# Tính trung bình theo (MovieID, AgeGroup)
avg_by_age = ratings_with_age.reduceByKey(sum_rating_and_count) \
                             .mapValues(calculate_average) \
                             .sortBy(get_sort_key)

results = avg_by_age.collect()

print("\n" + "="*75)
print("KẾT QUẢ BÀI 4: ĐIỂM TRUNG BÌNH THEO NHÓM TUỔI")
print("="*75)
print(f"  {'Tên phim':<40} {'Nhóm tuổi':<12} {'Điểm TB'}")
print("-"*75)
for (movie_id, age_group), avg in results:
    title = movie_title_broadcast.value.get(movie_id, f"MovieID {movie_id}")
    print(f"  {title:<40} {age_group:<12} {avg:.2f} sao")
print("="*75 + "\n")

sc.stop()