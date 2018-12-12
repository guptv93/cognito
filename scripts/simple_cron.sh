#!/bin/sh
echo "Starting Cognito Cron with RunAs User: $USER"
echo "Present working directory: $PWD"
export JAVA_HOME="/usr/java/latest"
echo "JAVA HOME: $JAVA_HOME"
rm -rf /tmp/stock_twit_latest.txt
stocklist="GOOGL MSFT AMZN AAPL FB"
for stock in $stocklist
do
    curl -X GET https://api.stocktwits.com/api/2/streams/symbol/${stock}.json | python /home/vvg239/RBDA/scripts/json_parser.py /tmp/stock_twit_latest.txt
done
hdfs dfs -rm -r cognito/stock_twit_latest.txt
hdfs dfs -put /tmp/stock_twit_latest.txt cognito
hdfs dfs -appendToFile /tmp/stock_twit_latest.txt cognito/archive.txt
hdfs dfs -rm -r cognito/newoutput
hadoop jar /home/vvg239/RBDA/agileJar.jar StockTwit cognito/stock_twit_latest.txt
hdfs dfs -cp cognito/newoutput/part-r-00000 cognito/hiveinput/`date +%s`
