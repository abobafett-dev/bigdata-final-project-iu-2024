#!/bin/bash
# Put all avsc files into HDFS warehouse
hdfs dfs -mkdir -p project/warehouse/avsc
hdfs dfs -put output/*.avsc project/warehouse/avsc

# Read password into variable
password=$(head -n 1 secrets/.hive.pass)

# Create database and tables from metadata
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team7 -p $password -f sql/db.hql > output/hive_results.txt

# Run queries for EDA
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team7 -p $password -f sql/q1.hql --hiveconf hive.resultset.use.unique.column.names=false > output/q1.csv
echo "store_nbr,avg_sales" > output/q1.csv
hdfs dfs -cat project/output/q1/* >> output/q1.csv

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team7 -p $password -f sql/q2.hql --hiveconf hive.resultset.use.unique.column.names=false > output/q2.csv
echo "family,avg_sales" > output/q2.csv
hdfs dfs -cat project/output/q2/* >> output/q2.csv

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team7 -p $password -f sql/q3.hql --hiveconf hive.resultset.use.unique.column.names=false > output/q3.csv
echo "dates,type,locale,avg_sales" > output/q3.csv
hdfs dfs -cat project/output/q3/* >> output/q3.csv

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team7 -p $password -f sql/q4.hql --hiveconf hive.resultset.use.unique.column.names=false > output/q4.csv
echo "dates,type,locale,avg_sales" > output/q4.csv
hdfs dfs -cat project/output/q4/* >> output/q4.csv

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team7 -p $password -f sql/q5.hql --hiveconf hive.resultset.use.unique.column.names=false > output/q5.csv
echo "dates,type,locale,avg_sales" > output/q5.csv
hdfs dfs -cat project/output/q5/* >> output/q5.csv