USE team7_projectdb;

CREATE TABLE IF NOT EXISTS evaluation (
                                          model varchar(200),
                                          RMSE float,
                                          R2 float
)
    row format delimited fields terminated by ','
    tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '../team7/project/output/evaluation.csv' OVERWRITE INTO TABLE evaluation;

CREATE TABLE IF NOT EXISTS model1_predictions (
                                                  label float,
                                                  prediction float,
                                                  dates date
)
    row format delimited fields terminated by ','
    tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '../team7/project/output/model1_predictions.csv' OVERWRITE INTO TABLE model1_predictions;

CREATE TABLE IF NOT EXISTS model2_predictions (
                                                  label float,
                                                  prediction float,
                                                  dates date
)
    row format delimited fields terminated by ','
    tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '../team7/project/output/model2_predictions.csv' OVERWRITE INTO TABLE model2_predictions;

CREATE TABLE IF NOT EXISTS model3_predictions (
                                                  label float,
                                                  prediction float,
                                                  dates date
)
    row format delimited fields terminated by ','
    tblproperties("skip.header.line.count"="1");
LOAD DATA INPATH '../team7/project/output/model3_predictions.csv' OVERWRITE INTO TABLE model3_predictions;