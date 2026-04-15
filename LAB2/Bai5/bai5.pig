data_raw = LOAD 'Bai1/output/words_no_stopwords/part-r-00000' 
           USING PigStorage('\t') AS (
               id: int, 
               word: chararray, 
               topic: chararray, 
               aspect: chararray, 
               sentiment: chararray
           );

data_proj = FOREACH data_raw GENERATE topic, word;

grouped_word = GROUP data_proj BY (topic, word);

word_counts = FOREACH grouped_word GENERATE 
    FLATTEN(group) AS (topic_name: chararray, word_name: chararray), 
    COUNT(data_proj) AS count_word;

category_grouped = GROUP word_counts BY topic_name;

top5_related = FOREACH category_grouped {
    sorted_words = ORDER word_counts BY count_word DESC;
    top5 = LIMIT sorted_words 5;
    GENERATE 
        group AS topic, 
        top5.(word_name, count_word) AS top_words;
};

STORE top5_related INTO 'output_b5/top5_related' USING PigStorage('\t');