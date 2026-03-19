bai1_data = LOAD 'bai1/bai1_output/part-r-00000' AS (id:int, word:chararray, category:chararray, aspect:chararray, sentiment:chararray);

word_freq = GROUP bai1_data BY word;
word_count = FOREACH word_freq GENERATE group as word, COUNT(bai1_data) as frequency;
word_sorted = ORDER word_count BY frequency DESC;
top5_words = LIMIT word_sorted 5;

dump top5_words;

category_group = GROUP bai1_data BY category;
category_stats = FOREACH category_group GENERATE group as category, COUNT(bai1_data) as count;
category_sorted = ORDER category_stats BY count DESC;

dump category_sorted;

aspect_group = GROUP bai1_data BY aspect;
aspect_stats = FOREACH aspect_group GENERATE group as aspect, COUNT(bai1_data) as count;
aspect_sorted = ORDER aspect_stats BY count DESC;

dump aspect_sorted;

STORE top5_words INTO 'bai2/top5_words' USING PigStorage('\t');
STORE category_sorted INTO 'bai2/stats_category' USING PigStorage('\t');
STORE aspect_sorted INTO 'bai2/stats_aspect' USING PigStorage('\t');
