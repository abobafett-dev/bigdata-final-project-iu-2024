#!/bin/bash
# Submit script to spark
spark-submit --master yarn scripts/stage3_scripts/ml_spark.py

# Extract files from HDFS after ML
hdfs dfs -cat project/data/train/*.json > data/train.json
hdfs dfs -cat project/data/test/*.json > data/test.json

hdfs dfs -get project/models/model1 models/model1
hdfs dfs -get project/models/model2 models/model2
hdfs dfs -get project/models/model3 models/model3

hdfs dfs -cat project/output/model1_predictions.csv/*.csv > output/model1_predictions.csv
hdfs dfs -cat project/output/model2_predictions.csv/*.csv > output/model2_predictions.csv
hdfs dfs -cat project/output/model3_predictions.csv/*.csv > output/model3_predictions.csv
hdfs dfs -cat project/output/evaluation.csv/*.csv > output/evaluation.csv