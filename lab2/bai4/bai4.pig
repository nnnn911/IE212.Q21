bai1_data = LOAD 'bai1/bai1_output/part-r-00000' AS (id:int, word:chararray, category:chararray, aspect:chararray, sentiment:chararray);

grouped = GROUP bai1_data BY (category, sentiment, word);
grouped_with_count = FOREACH grouped GENERATE group.category as category, group.sentiment as sentiment, group.word as word, COUNT(bai1_data) as count;

positive_data = FILTER grouped_with_count BY sentiment == 'positive';
positive_by_cat = GROUP positive_data BY category;
top5_positive = FOREACH positive_by_cat {
    sorted = ORDER positive_data BY count DESC;
    limited = LIMIT sorted 5;
    GENERATE FLATTEN(limited);
};

negative_data = FILTER grouped_with_count BY sentiment == 'negative';
negative_by_cat = GROUP negative_data BY category;
top5_negative = FOREACH negative_by_cat {
    sorted = ORDER negative_data BY count DESC;
    limited = LIMIT sorted 5;
    GENERATE FLATTEN(limited);
};

dump top5_positive;
dump top5_negative;

STORE top5_positive INTO 'bai4/top5_positive_by_category' USING PigStorage('\t');
STORE top5_negative INTO 'bai4/top5_negative_by_category' USING PigStorage('\t');
