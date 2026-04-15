rmf output_b3/neg;
rmf output_b3/pos;

data_raw = LOAD 'hotel-review.csv' USING PigStorage(';')
    AS (id: chararray, review: chararray, category: chararray, aspect: chararray, sentiment: chararray);

valid_data = FILTER data_raw BY id IS NOT NULL 
             AND aspect IS NOT NULL 
             AND sentiment IS NOT NULL
             AND id != 'id'; -- Bỏ qua dòng tiêu đề

data = FOREACH valid_data GENERATE
    aspect,
    LOWER(TRIM(sentiment)) AS sentiment;

-- =====================================
-- XỬ LÝ NEGATIVE
-- =====================================
neg_filter = FILTER data BY sentiment == 'negative';
neg_group = GROUP neg_filter BY aspect;
neg_count = FOREACH neg_group GENERATE
    group AS aspect,
    COUNT(neg_filter) AS cnt;

neg_all = GROUP neg_count ALL;
top_neg = FOREACH neg_all {
    result = TOP(1, 1, neg_count);
    GENERATE FLATTEN(result);
};

-- =====================================
-- XỬ LÝ POSITIVE
-- =====================================
pos_filter = FILTER data BY sentiment == 'positive';
pos_group = GROUP pos_filter BY aspect;
pos_count = FOREACH pos_group GENERATE
    group AS aspect,
    COUNT(pos_filter) AS cnt;

pos_all = GROUP pos_count ALL;
top_pos = FOREACH pos_all {
    result = TOP(1, 1, pos_count);
    GENERATE FLATTEN(result);
};

STORE top_neg INTO 'output_b3/neg' USING PigStorage('\t');
STORE top_pos INTO 'output_b3/pos' USING PigStorage('\t');