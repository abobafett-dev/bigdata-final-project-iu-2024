"""Main sparkML script"""

import os
import math
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.regression import LinearRegression, GBTRegressor, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import when, avg, coalesce, date_format, sin, cos

# Add here your team number teamx
TEAM = "team7"

# location of your Hive database in HDFS
WAREHOUSE = "project/hive/warehouse"

spark = SparkSession.builder\
    .appName(f"{TEAM} - spark ML")\
    .master("yarn")\
    .config("spark.executor.instances", 8)\
    .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")\
    .config("spark.sql.warehouse.dir", WAREHOUSE)\
    .config("spark.sql.avro.compression.codec", "snappy")\
    .enableHiveSupport()\
    .getOrCreate()

sc = spark.sparkContext

main = spark.read.format("avro").table('team7_projectdb.main_part')
oil = spark.read.format("avro").table('team7_projectdb.oil')
hol_events = spark.read.format("avro").table('team7_projectdb.holidays_events')
stores = spark.read.format("avro").table('team7_projectdb.stores')
transactions = spark.read.format("avro").table('team7_projectdb.transactions')

encode_main = ['family']
encode_hol_events = ['type', 'locale', 'locale_name']
encode_stores = ['city', 'state', 'type_store']

# Table main
# Encode categorical features in table main
indexers_main = [StringIndexer(inputCol=c, outputCol=f"{c}_indexed")
                 .setHandleInvalid("skip") for c in encode_main]
pipeline = Pipeline(stages=indexers_main)
main = pipeline.fit(main).transform(main).drop(*encode_main + ["id"])

# Table oil
# Fill missing values in oil table with average of neighbors
window = Window.rowsBetween(-1, 1)
oil = oil.withColumn("avg_dcoilwtico", avg(oil["dcoilwtico"]).over(window))
oil = oil.withColumn("dcoilwtico", coalesce(oil["dcoilwtico"], oil["avg_dcoilwtico"]))
oil = oil.drop(*["avg_dcoilwtico", "id"])

# Table stores
# Encode categorical features in table stores
stores = stores.withColumnRenamed("type", "type_store")
indexers_stores = [StringIndexer(inputCol=c, outputCol=f"{c}_indexed")
                   .setHandleInvalid("skip") for c in encode_stores]
pipeline = Pipeline(stages=indexers_stores)
stores = pipeline.fit(stores).transform(stores).drop(*encode_stores)

# Table transactions
transactions = transactions.drop("id")

# Join tables
transformed = main \
    .join(oil, on="dates", how="left") \
    .join(transactions, on=["dates", "store_nbr"], how="left") \
    .join(stores, on="store_nbr", how="left") \

# Split dates to year, month, day
transformed = transformed \
    .withColumn("year", date_format("dates", "yyyy").cast('int')) \
    .withColumn("month", date_format("dates", "MM").cast('int')) \
    .withColumn("day", date_format("dates", "dd").cast('int')) \
    .drop("dates")

# Sort dataframe by date
transformed = transformed.sort(["year", "month", "day"])

# Fill na
transformed = transformed.fillna(0)

# Encode cyclical month and days
transformed = transformed.withColumn("month_sin", sin(2 * math.pi * transformed.month / 12))
transformed = transformed.withColumn("month_cos", cos(2 * math.pi * transformed.month / 12))
transformed = transformed.withColumn("day_sin", sin(2 * math.pi * transformed.day / 31))
transformed = transformed.withColumn("day_cos", cos(2 * math.pi * transformed.day / 31))
transformed = transformed.drop(*["month", "day"])

# Assemble all features into single column
input_cols = [i for i in transformed.schema.names if i != "sales"]
assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
pipeline = Pipeline(stages=[assembler])
transformed = pipeline.fit(transformed).transform(transformed)
transformed = transformed.select(["sales", "features"]).withColumnRenamed("sales", "label")

# Split the data to train/test 80/20
train_data = transformed.limit(int(transformed.count() * 0.8))
test_data = transformed.subtract(train_data)


def run(command):
    """Function to run commands"""
    return os.popen(command).read()


train_data.select("features", "label")\
    .coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("json")\
    .save("project/data/train")

# Run it from root directory of the repository
run("hdfs dfs -cat project/data/train/*.json > data/train.json")

test_data.select("features", "label")\
    .coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("json")\
    .save("project/data/test")

# Run it from root directory of the repository
run("hdfs dfs -cat project/data/test/*.json > data/test.json")

# Create Linear Regression Model
lr = LinearRegression()


def pred_cut(pred):
    """Function to cut predictions below 0"""
    return pred\
        .withColumn("prediction", when(
            pred.prediction < 0, 0
                          )
                    .otherwise(pred.prediction))


# Fit the data to the pipeline stages
model_lr = lr.fit(train_data)

predictions = model_lr.transform(test_data)
predictions = pred_cut(predictions)

# Evaluate the performance of the model
evaluator1_rmse = RegressionEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="rmse"
)
evaluator1_r2 = RegressionEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="r2"
)

rmse = evaluator1_rmse.evaluate(predictions)
r2 = evaluator1_r2.evaluate(predictions)

grid = ParamGridBuilder()
grid = grid.addGrid(model_lr.aggregationDepth, [2, 3, 4])\
           .addGrid(model_lr.loss, ["squaredError", "huber"])\
           .build()

cv = CrossValidator(estimator=lr,
                    estimatorParamMaps=grid,
                    evaluator=evaluator1_r2,
                    parallelism=5,
                    numFolds=3)

cvModel = cv.fit(train_data)
bestModel = cvModel.bestModel

model1 = bestModel

model1.write().overwrite().save("project/models/model1")

# Run it from root directory of the repository
run("hdfs dfs -get project/models/model1 models/model1")

predictions = model1.transform(test_data)
predictions = pred_cut(predictions)

predictions.select("label", "prediction")\
    .coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep", ",")\
    .option("header", "true")\
    .save("project/output/model1_predictions.csv")

# Run it from root directory of the repository
run("hdfs dfs -cat project/output/model1_predictions.csv/*.csv > output/model1_predictions.csv")

rmse1 = evaluator1_rmse.evaluate(predictions)
r21 = evaluator1_r2.evaluate(predictions)

# Create GBT Model
gbt = GBTRegressor(maxBins=4993, seed=42)

# Fit the data to the pipeline stages
model_gbt = gbt.fit(train_data)
predictions = model_gbt.transform(test_data)
predictions = pred_cut(predictions)

# Evaluate the performance of the model
evaluator2_rmse = RegressionEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="rmse"
)
evaluator2_r2 = RegressionEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="r2"
)

grid = ParamGridBuilder()
grid = grid.addGrid(model_gbt.maxIter, [10, 100, 500]).build()

cv = CrossValidator(estimator=gbt,
                    estimatorParamMaps=grid,
                    evaluator=evaluator2_rmse,
                    parallelism=5,
                    numFolds=3)

cvModel = cv.fit(train_data)
bestModel = cvModel.bestModel

model2 = bestModel

model2.write().overwrite().save("project/models/model2")

# Run it from root directory of the repository
run("hdfs dfs -get project/models/model2 models/model2")

predictions = model2.transform(test_data)
predictions = pred_cut(predictions)

predictions.select("label", "prediction")\
    .coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep", ",")\
    .option("header", "true")\
    .save("project/output/model2_predictions.csv")

# Run it from root directory of the repository
run("hdfs dfs -cat project/output/model2_predictions.csv/*.csv > output/model2_predictions.csv")

rmse2 = evaluator2_rmse.evaluate(predictions)
r22 = evaluator2_r2.evaluate(predictions)

# Create Random Forest Model
rfr = RandomForestRegressor(maxBins=4993, seed=42)
model_rfr = gbt.fit(train_data)

predictions = model_rfr.transform(test_data)
predictions = pred_cut(predictions)

# Evaluate the performance of the model
evaluator3_rmse = RegressionEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="rmse"
)
evaluator3_r2 = RegressionEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="r2"
)

grid = ParamGridBuilder()
grid = grid\
    .addGrid(model_rfr.maxDepth, [2, 5])\
    .addGrid(model_rfr.lossType, ['squared', 'absolute'])\
    .build()

cv = CrossValidator(estimator=rfr,
                    estimatorParamMaps=grid,
                    evaluator=evaluator3_rmse,
                    parallelism=5,
                    numFolds=3)

cvModel = cv.fit(train_data)
bestModel = cvModel.bestModel

model3 = bestModel
model3.write().overwrite().save("project/models/model3")

# Run it from root directory of the repository
run("hdfs dfs -get project/models/model3 models/model3")

predictions = model3.transform(test_data)
predictions = pred_cut(predictions)

predictions.select("label", "prediction")\
    .coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep", ",")\
    .option("header", "true")\
    .save("project/output/model3_predictions.csv")

# Run it from root directory of the repository
run("hdfs dfs -cat project/output/model3_predictions.csv/*.csv > output/model3_predictions.csv")

rmse3 = evaluator3_rmse.evaluate(predictions)
r23 = evaluator3_r2.evaluate(predictions)

models = [[str(model1), rmse1, r21], [str(model2), rmse2, r22], [str(model3), rmse3, r23]]

df = spark.createDataFrame(models, ["model", "RMSE", "R2"])

df.coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep", ",")\
    .option("header", "true")\
    .save("project/output/evaluation.csv")

# Run it from root directory of the repository
run("hdfs dfs -cat project/output/evaluation.csv/*.csv > output/evaluation.csv")
