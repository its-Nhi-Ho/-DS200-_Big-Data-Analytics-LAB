data_raw = LOAD 'hotel-review.csv' USING PigStorage(';')
    AS (id: int, review: chararray, topic: chararray, aspect: chararray, sentiment: chararray);

words_data = LOAD 'Bai1/output/words_no_stopwords' USING PigStorage('\t')
    AS (id: int, word: chararray, topic: chararray, aspect: chararray, sentiment: chararray);

word_groups = GROUP words_data BY word;
word_counts = FOREACH word_groups GENERATE
    group as word,
    COUNT(words_data) AS frequency;

high_freq_words = FILTER word_counts BY frequency > 500;

high_freq_words_sorted = ORDER high_freq_words BY frequency DESC;

STORE high_freq_words_sorted INTO 'output_b2/word_freq_over_500' USING PigStorage('\t');

topic_groups = GROUP data_raw BY topic;
topic_counts = FOREACH topic_groups GENERATE
    group AS topic,
    COUNT(data_raw) AS num_comments;

STORE topic_counts INTO 'output_b2/topic_counts' USING PigStorage('\t');

aspect_groups = GROUP data_raw BY aspect;
aspect_counts = FOREACH aspect_groups GENERATE
    group AS aspect,
    COUNT(data_raw) AS num_comments;

STORE aspect_counts INTO 'output_b2/aspect_counts' USING PigStorage('\t');

DUMP topic_counts;
DUMP aspect_counts;