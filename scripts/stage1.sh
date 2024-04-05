#!/bin/bash
# Unzip data (data already inside github repo)
rm data/*.csv
unzip data/dataset.zip -d data/

# Run python script to create tables, import data and test database
python3 scripts/stage1-scripts/build_projectdb.py

# Import password as variable
password=$(head -n 1 secrets/.psql.pass)

# Delete anything from warehouse
hdfs dfs -rm -R -skipTrash project/warehouse/*

# Compress data and put it inside HDFS
sqoop import-all-tables --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team7_projectdb --username team7 --password $password --compression-codec=snappy --compress --as-avrodatafile --warehouse-dir=project/warehouse --m 1