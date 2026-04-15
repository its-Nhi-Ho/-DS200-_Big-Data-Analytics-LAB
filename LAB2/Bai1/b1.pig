-- Load dữ liệu
data = LOAD 'hotel-review.csv' USING PigStorage(';') 
    AS (id: int, review: chararray, topic: chararray, aspect: chararray, sentiment: chararray);

-- Bước 1: Lowercase (Giữ lại id)
data_lower = FOREACH data GENERATE
    id AS id,
    LOWER(review) AS review,
    topic AS topic,
    aspect AS aspect,
    sentiment AS sentiment;

-- Bước 2: Loại ký tự đặc biệt
data_clean_1 = FOREACH data_lower GENERATE
    id AS id,
    REPLACE(review, '[@\'\\^!?,.]', ' ') AS review,
    topic AS topic,
    aspect AS aspect,
    sentiment AS sentiment;

data_clean_2 = FOREACH data_clean_1 GENERATE
    id AS id,
    REPLACE(review, '[0-9%\\-?+/&:~<>=]', ' ') AS review,
    topic AS topic,
    aspect AS aspect,
    sentiment AS sentiment;

-- Bước 3: Tokenize (tách từ)
words = FOREACH data_clean_2 GENERATE
    id AS id,
    FLATTEN(TOKENIZE(review)) AS word,
    topic AS topic,
    aspect AS aspect,
    sentiment AS sentiment;

-- Lưu tạm (nếu cần)
-- STORE words INTO 'output/words' USING PigStorage('\t');

-- Bước 4: Load stopwords 
stopwords = LOAD 'stopwords.txt' USING PigStorage('\n') AS (word: chararray);

-- Bước 5: LEFT OUTER JOIN để loại stopwords
words_joined = JOIN words BY word LEFT OUTER, stopwords BY word;

-- Bước 6: Giữ lại từ KHÔNG có trong stopwords
words_filtered = FILTER words_joined BY stopwords::word IS NULL;

-- Bước 7: Lấy các cột cần thiết (gồm cả id)
words_without_stopwords = FOREACH words_filtered GENERATE
    words::id AS id,
    words::word AS word,
    words::topic AS topic,
    words::aspect AS aspect,
    words::sentiment AS sentiment;

STORE words_without_stopwords INTO 'output/words_no_stopwords' USING PigStorage('\t');

samples = LIMIT words_without_stopwords 10;
DUMP samples;