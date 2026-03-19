stopwords = LOAD 'stopwords.txt' AS (stopword:chararray);
stopwords_lower = FOREACH stopwords GENERATE LOWER(stopword) as stopword;

reviews = LOAD 'hotel-review.csv' USING PigStorage(';') 
    AS (id:int, text:chararray, category:chararray, aspect:chararray, sentiment:chararray);

reviews_lower = FOREACH reviews GENERATE id, LOWER(text) as text_lower, category, aspect, sentiment;

reviews_tokenized = FOREACH reviews_lower GENERATE id, FLATTEN(TOKENIZE(text_lower)) as word, category, aspect, sentiment;

reviews_cleaned = FILTER reviews_tokenized BY word != '';

reviews_cleaned_regex = FOREACH reviews_cleaned GENERATE 
    id, 
    REGEX_EXTRACT(word, '^(.*?)[.!?,;:]*$', 1) as word_clean,
    category,
    aspect,
    sentiment;

reviews_cleaned_words = FILTER reviews_cleaned_regex BY word_clean IS NOT NULL AND word_clean != '';

reviews_cleaned_fixed = FOREACH reviews_cleaned_words GENERATE 
    id, 
    word_clean as word,
    category,
    aspect,
    sentiment;

cogrouped = COGROUP reviews_cleaned_fixed BY word, stopwords_lower BY stopword;

no_stopwords = FILTER cogrouped BY SIZE(stopwords_lower) == 0;

result = FOREACH no_stopwords GENERATE FLATTEN(reviews_cleaned_fixed);

dump result;
STORE result INTO 'bai1/bai1_output' USING PigStorage('\t');


