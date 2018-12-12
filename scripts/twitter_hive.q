CREATE EXTERNAL TABLE IF NOT EXISTS tweets_data (created_at_et STRING, tweet STRING, retweets INT, user_id STRING, user_verified BOOLEAN, total_tweets INT, total_followers INT, total_following INT, symbol STRING, stanford_sentiment INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="||")
STORED AS TEXTFILE
LOCATION '/user/vvg239/cognito/hiveInputTwitter';

CREATE TABLE IF NOT EXISTS tweets
(tweet string, retweets INT, user_id STRING, user_verified BOOLEAN,total_followers INT, symbol STRING, stanford_sentiment INT)
PARTITIONED BY (created_date date,    created_hour int);

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;

INSERT OVERWRITE TABLE tweets
PARTITION (created_date, created_hour)
SELECT    tweet,    retweets,    user_id,    user_verified,    total_followers,    symbol,    stanford_sentiment- 2, substr(created_at_et, 1, 10), substr(created_at_et, 12, 2)
FROM    tweets_data;

CREATE TABLE IF NOT EXISTS stanford_prediction
(created_date date, created_hour int, symbol string, prediction double);

INSERT OVERWRITE TABLE stanford_prediction
SELECT created_date, created_hour, symbol, AVG(stanford_sentiment*log10(total_followers)) FROM tweets GROUP BY created_date, created_hour, symbol;

CREATE TABLE IF NOT EXISTS split_tweets(created_date date, created_hour int, symbol string, tweets_words array<string>);

INSERT OVERWRITE TABLE split_tweets SELECT created_date, created_hour, symbol, split(tweet,' ') FROM tweets;

CREATE TABLE IF NOT EXISTS tweet_words(created_date date, created_hour int, symbol string, tweet_word string);


INSERT OVERWRITE TABLE tweet_words SELECT created_date, created_hour, symbol, word FROM split_tweets LATERAL VIEW explode(tweets_words) w AS word;

CREATE EXTERNAL TABLE IF NOT EXISTS  dictionary(word string,rating int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA INPATH '/user/vvg239/cognito/AFINN.txt' INTO TABLE dictionary;

CREATE TABLE IF NOT EXISTS tweetword_join (created_date date, created_hour int, symbol string, tweet_word string, rating int);

INSERT OVERWRITE TABLE tweetword_join SELECT tweet_words.created_date, tweet_words.created_hour, tweet_words.symbol, tweet_words.tweet_word, dictionary.rating FROM tweet_words LEFT OUTER JOIN dictionary on(tweet_words.tweet_word=dictionary.word);

CREATE TABLE IF NOT EXISTS afinn_prediction (created_date date, created_hour int, symbol string, rating double);

INSERT OVERWRITE TABLE afinn_prediction SELECT created_date, created_hour,symbol,AVG(rating) as rating from tweetword_join GROUP BY created_date, created_hour, symbol;
