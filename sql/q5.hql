USE team7_projectdb;

DROP TABLE IF EXISTS q5_results;
CREATE EXTERNAL TABLE q5_results(
                                    day_type VARCHAR(10),
                                    family VARCHAR(30),
                                    sales FLOAT)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    location 'project/hive/warehouse/q5';

-- to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO q5_results
SELECT
    CASE WHEN he.type = 'Holiday' AND he.transferred = FALSE THEN 'Workday' ELSE 'Holiday' END AS day_type,
    m.family,
    m.sales
FROM main_part AS m
         JOIN holidays_events AS he ON m.dates = he.dates;

INSERT OVERWRITE DIRECTORY 'project/output/q5'
    ROW FORMAT DELIMITED FIELDS
    TERMINATED BY ','
SELECT * FROM q5_results;