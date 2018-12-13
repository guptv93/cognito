# Cognito
## Sentiment Analytics on Twitter and StockTwits Data for Stock Price Prediction Using Big Data Tools

The code is divided into 3 sections:
1. MapReduce code
2. Stanford NLP code
3. Scripts

The execution flow starts with the `scripts` folder, basically from the simple_cron.sh file. We have configured the cron process to run simple_cron.sh every hour at 55 minutes. The instructions in the file can be run manually to test the code.

The simple_cron issues a curl request to fetch StockTwits data. This data is passed to json_parser.py, which converts the data from json format to pipe (||) separated values format. json_parser.py also removes new-line characters from inside the tweet body. Finally this formatted data is uploaded to HDFS in the /user/vvg239/cognito, and is also appended to an archive file for backup.

The MapReduce code is in the Cognito/MapReduce github directory. It first filters the tweets, removes duplicates and keeps only the tweets for the past 1 hour. Simultaneously it also removes links and emojis from the tweets. The MapReduce code also extracts named key entities from the tweets. It does so by calling the StanfordNLP code in Cognito/StanfordNLP folder. The final output is put in the directory /user/vvg239/cognito/newoutput directory. 

From this directory the output is moved to the location of hive input table (/user/vvg239/cognito/hiveinput). Cognito/scripts/stock_twit.q file contains all the hive instructions for the analytics.

A similar process runs in parallel to fetch Twitter data. The final cleaned, partitioned twitter data is placed in /user/vvg239/cognito/hiveInputTwitter folder which is the location of hive external table.
