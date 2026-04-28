import os

os.environ["PYSPARK_PYTHON"] = r"D:\Uni\junior\DS200\LAB3\venv_new\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Uni\junior\DS200\LAB3\venv_new\Scripts\python.exe"

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Lab3_Bai5").setMaster("local[*]") \
    .set("spark.python.worker.faulthandler.enabled", "true")
sc = SparkContext(conf=conf)

DELIMITER = ","

# ==========================================
# ĐỊNH NGHĨA CÁC HÀM XỬ LÝ
# ==========================================

def parse_line(line):
    return line.split(DELIMITER)

def extract_occupation_name(parts):
    """Trả về (OccupationID, OccupationName)"""
    return (parts[0], parts[1])

def extract_user_occupation(parts):
    """Trả về (UserID, OccupationID)"""
    return (parts[0], parts[3])  # UserID, OccupationID

def extract_rating_info(parts):
    """Trả về (UserID, Rating)"""
    return (parts[0], float(parts[2]))

def attach_occupation(record):
    """
    Nhận (UserID, Rating)
    Trả về (OccupationName, (Rating, 1))
    """
    user_id = record[0]
    rating = record[1]
    occ_id = user_occ_broadcast.value.get(user_id, "0")
    occ_name = occ_name_broadcast.value.get(occ_id, "Unknown")
    return (occ_name, (rating, 1))

def sum_rating_and_count(v1, v2):
    return (v1[0] + v2[0], v1[1] + v2[1])

def calculate_avg_and_count(value):
    avg = round(value[0] / value[1], 2)
    return (avg, value[1])

def get_sort_key(record):
    """Sắp xếp theo điểm TB giảm dần"""
    return record[1][0]


# Đọc occupation.txt: OccupationID -> OccupationName
occ_rdd = sc.textFile("occupation.txt")
occ_name_dict = occ_rdd.map(parse_line) \
                       .map(extract_occupation_name) \
                       .collectAsMap()

occ_name_broadcast = sc.broadcast(occ_name_dict)

# Đọc users.txt: UserID -> OccupationID
users_rdd = sc.textFile("users.txt")
user_occ_dict = users_rdd.map(parse_line) \
                         .map(extract_user_occupation) \
                         .collectAsMap()

user_occ_broadcast = sc.broadcast(user_occ_dict)

# Đọc ratings, gán nghề nghiệp
ratings_rdd = sc.textFile("ratings_1.txt,ratings_2.txt")
ratings_with_occ = ratings_rdd.map(parse_line) \
                              .map(extract_rating_info) \
                              .map(attach_occupation)

# Reduce và tính trung bình
avg_by_occ = ratings_with_occ.reduceByKey(sum_rating_and_count) \
                             .mapValues(calculate_avg_and_count) \
                             .sortBy(get_sort_key, ascending=False)

results = avg_by_occ.collect()

print("\n" + "="*60)
print("KẾT QUẢ BÀI 5: ĐIỂM TRUNG BÌNH THEO NGHỀ NGHIỆP")
print("="*60)
print(f"  {'Nghề nghiệp':<20} {'Điểm TB':<12} {'Số lượt'}")
print("-"*60)
for occ, (avg, count) in results:
    print(f"  {occ:<20} {avg:<12.2f} {count} lượt")
print("="*60 + "\n")

sc.stop()