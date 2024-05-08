USE team7_projectdb;

DROP TABLE IF EXISTS q1_results;
CREATE EXTERNAL TABLE q1_results(
                                    family VARCHAR(30),
                                    sales FLOAT)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    location 'project/hive/warehouse/q1';

-- to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO q1_results
SELECT family, sales
FROM main_part;

INSERT OVERWRITE DIRECTORY 'project/output/q1'
    ROW FORMAT DELIMITED FIELDS
    TERMINATED BY ','
SELECT * FROM q1_results;