import os
import sys

os.environ["PYSPARK_PYTHON"] = r"D:\Uni\junior\DS200\LAB3\venv_new\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\Uni\junior\DS200\LAB3\venv_new\Scripts\python.exe"

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Lab3_Bai2").setMaster("local[*]") \
    .set("spark.python.worker.faulthandler.enabled", "true")
sc = SparkContext(conf=conf)

DELIMITER = ","


def parse_line(line):
    """Cắt dòng text thành list các chuỗi dựa trên DELIMITER"""
    return line.split(DELIMITER)

def extract_movie_genres(parts):
    """Trả về (MovieID, [Genre1, Genre2, ...])"""
    movie_id = parts[0]
    genres = parts[2].split("|")
    return (movie_id, genres)

def extract_rating_info(parts):
    """Trả về (MovieID, Rating)"""
    movie_id = parts[1]
    rating = float(parts[2])
    return (movie_id, rating)

def expand_genres(record):
    """
    Nhận (MovieID, (Rating, [Genre1, Genre2, ...]))
    Trả về list các (Genre, (Rating, 1))
    """
    rating = record[1][0]
    genres = record[1][1]
    result = []
    for genre in genres:
        result.append((genre, (rating, 1)))
    return result

def sum_rating_and_count(value1, value2):
    """Cộng dồn tổng rating và số lượt"""
    return (value1[0] + value2[0], value1[1] + value2[1])

def calculate_average(value):
    """Tính trung bình từ (Tổng Rating, Số Lượt)"""
    return round(value[0] / value[1], 2)

def get_sort_key(record):
    """Lấy điểm trung bình để sắp xếp"""
    return record[1]


# Tạo map MovieID -> List of Genres
movies_rdd = sc.textFile("movies.txt")
movie_genres_dict = movies_rdd.map(parse_line) \
                              .map(extract_movie_genres) \
                              .collectAsMap()

movie_genres_broadcast = sc.broadcast(movie_genres_dict)

#  Đọc ratings, map MovieID -> Rating
ratings_rdd = sc.textFile("ratings_1.txt,ratings_2.txt")
movie_ratings = ratings_rdd.map(parse_line) \
                           .map(extract_rating_info)

# Join với genres: (MovieID, Rating) -> (MovieID, (Rating, [Genres]))
def attach_genres(record):
    movie_id = record[0]
    rating = record[1]
    genres = movie_genres_broadcast.value.get(movie_id, [])
    return (movie_id, (rating, genres))

movie_with_genres = movie_ratings.map(attach_genres)

# Expand ra (Genre, (Rating, 1)) rồi tính trung bình
genre_ratings = movie_with_genres.flatMap(expand_genres)
reduced = genre_ratings.reduceByKey(sum_rating_and_count)
avg_by_genre = reduced.mapValues(calculate_average)

# Sắp xếp theo điểm trung bình giảm dần
sorted_genres = avg_by_genre.sortBy(get_sort_key, ascending=False).collect()

print("\n" + "="*50)
print("KẾT QUẢ BÀI 2: ĐIỂM TRUNG BÌNH THEO THỂ LOẠI")
print("="*50)
for genre, avg in sorted_genres:
    print(f"  {genre:<20}: {avg:.2f} sao")
print("="*50 + "\n")

sc.stop()