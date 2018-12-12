CREATE EXTERNAL TABLE IF NOT EXISTS stocktwit_data
(created_at string, id string, symbol string, body string, sentiment string, user_name string, user_official boolean, user_followers int, entities array<string>)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ("field.delim"="||","timestamp.formats"="yyyy-MM-dd'T'HH:mm:ssZ","collection.delim"=",")
STORED AS TEXTFILE
LOCATION '/user/vvg239/cognito/hiveinput/';

SELECT * FROM stocktwit_data LIMIT 5;

CREATE TABLE IF NOT EXISTS stocktwit
(id string, symbol string, body string, sentiment int, user_name string, user_official boolean, user_followers int, entities array<string>)
PARTITIONED BY (created_date date, created_hour int);

INSERT OVERWRITE TABLE stocktwit
PARTITION (created_date, created_hour)
SELECT
   id,
   symbol,
   body,
   case sentiment
   when 'Bullish' then 1
   when 'Bearish' then -1
   else 0 
   end as sentiment,
   user_name,
   user_official,
   user_followers,
   entities,
   cast(substr(created_at, 12, 2) as int),
   cast(substr(created_at, 12, 2) as int)
from 
   stocktwit_data;

CREATE TABLE IF NOT EXISTS stocktwit_entities
(created_date date, created_hour int, symbol string, entities array<string>);

INSERT OVERWRITE TABLE stocktwit_entities
SELECT created_date, created_hour, symbol, collect_set(exploded_entities) FROM 
   stocktwit LATERAL VIEW explode(entities) e as exploded_entities
GROUP BY created_date, created_hour, symbol;

CREATE TABLE IF NOT EXISTS stocktwit_prediction
(created_date date, created_hour int, symbol string, prediction int);

INSERT OVERWRITE TABLE stocktwit_prediction
SELECT created_date, created_hour, symbol, AVG(sentiment*log10(user_followers)) FROM stocktwit
WHERE sentiment <> 0 GROUP BY created_date, created_hour, symbol;
