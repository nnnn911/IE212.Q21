bai1_data = LOAD 'bai1/bai1_output/part-r-00000' AS (id:int, word:chararray, category:chararray, aspect:chararray, sentiment:chararray);

aspect_sentiment_group = GROUP bai1_data BY (aspect, sentiment);
aspect_sentiment_stats = FOREACH aspect_sentiment_group GENERATE group as key, COUNT(bai1_data) as count;

negative_group = FOREACH aspect_sentiment_stats GENERATE key.aspect as aspect, key.sentiment as sentiment, count;
negative_filtered = FILTER negative_group BY sentiment == 'negative';
negative_sorted = ORDER negative_filtered BY count DESC;
top_negative = LIMIT negative_sorted 1;

dump top_negative;

positive_group = FOREACH aspect_sentiment_stats GENERATE key.aspect as aspect, key.sentiment as sentiment, count;
positive_filtered = FILTER positive_group BY sentiment == 'positive';
positive_sorted = ORDER positive_filtered BY count DESC;
top_positive = LIMIT positive_sorted 1;

dump top_positive;

STORE top_negative INTO 'bai3/top_negative_aspect' USING PigStorage('\t');
STORE top_positive INTO 'bai3/top_positive_aspect' USING PigStorage('\t');
