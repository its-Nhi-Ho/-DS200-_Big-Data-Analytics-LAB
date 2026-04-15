-- 1. Đọc dữ liệu từ kết quả Bài 1
data_raw = LOAD 'Bai1/output/words_no_stopwords/part-r-00000' 
           USING PigStorage('\t') AS (
               id: int, 
               word: chararray, 
               topic: chararray, 
               aspect: chararray, 
               sentiment: chararray
           );

pos_data = FILTER data_raw BY sentiment == 'positive';

pos_grouped = GROUP pos_data BY (topic, word);

pos_word_counts = FOREACH pos_grouped GENERATE 
    group.$0 AS topic, 
    group.$1 AS word, 
    COUNT(pos_data) AS count_pos;

pos_category_grouped = GROUP pos_word_counts BY topic;

top5_pos = FOREACH pos_category_grouped {
    sorted_pos = ORDER pos_word_counts BY count_pos DESC;
    top5 = LIMIT sorted_pos 5;
    GENERATE 
        group AS topic, -- Vì chỉ group by 1 trường nên group chính là topic
        top5.(word, count_pos) AS top_positive_words;
};


neg_data = FILTER data_raw BY sentiment == 'negative';

neg_grouped = GROUP neg_data BY (topic, word);

neg_word_counts = FOREACH neg_grouped GENERATE 
    group.$0 AS topic, 
    group.$1 AS word, 
    COUNT(neg_data) AS count_neg;

neg_category_grouped = GROUP neg_word_counts BY topic;

top5_neg = FOREACH neg_category_grouped {
    sorted_neg = ORDER neg_word_counts BY count_neg DESC;
    top5 = LIMIT sorted_neg 5;
    GENERATE 
        group AS topic, 
        top5.(word, count_neg) AS top_negative_words;
};

STORE top5_pos INTO 'output_b4/top5_positive' USING PigStorage('\t');
STORE top5_neg INTO 'output_b4/top5_negative' USING PigStorage('\t');