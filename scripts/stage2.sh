#!/bin/bash
# Put all avsc filis into HDFS warehouse
hdfs dfs -mkdir -p project/warehouse/avsc
hdfs dfs -put output/*.avsc project/warehouse/avsc

# Read password into variable
password=$(head -n 1 secrets/.hive.pass)

# Create database and tables from metadata
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team7 -p $password -f sql/db.hql > output/hive_results.txt

# Run queries for EDA
#beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team7 -p $password -f sql/q1.hql
#beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team7 -p $password -f sql/q2.hql
#beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team7 -p $password -f sql/q3.hql
#beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team7 -p $password -f sql/q4.hql
#beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team7 -p $password -f sql/q5.hql