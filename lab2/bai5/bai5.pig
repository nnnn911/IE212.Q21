bai1_data = LOAD 'bai1/bai1_output/part-r-00000' AS (id:int, word:chararray, category:chararray, aspect:chararray, sentiment:chararray);

grouped = GROUP bai1_data BY (category, word);
grouped_with_count = FOREACH grouped GENERATE group.category as category, group.word as word, COUNT(bai1_data) as count;

by_category = GROUP grouped_with_count BY category;
top5_by_category = FOREACH by_category {
    sorted = ORDER grouped_with_count BY count DESC;
    limited = LIMIT sorted 5;
    GENERATE FLATTEN(limited);
};

dump top5_by_category;

STORE top5_by_category INTO 'bai5/top5_words_by_category' USING PigStorage('\t');
