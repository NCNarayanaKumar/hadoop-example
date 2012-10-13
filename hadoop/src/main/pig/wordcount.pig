comments = LOAD '/users/junittest/comments' USING com.welflex.hadoop.pig.load.PigJsonLoader() AS (commentMap:map[]);
words = FOREACH comments GENERATE FLATTEN(TOKENIZE(LOWER(commentMap#'comment')));
filter_words = FILTER words BY com.welflex.hadoop.pig.func.LikeFilter($0, '2012', 'bond', 'cave', 'james', 'walther');
grouped = GROUP filter_words BY $0;
counts = FOREACH grouped GENERATE group,COUNT(filter_words);
store counts INTO '/users/junittest/wcresults';
