USE team7_projectdb;

DROP TABLE IF EXISTS q4_results;
CREATE EXTERNAL TABLE q4_results(
                                    store_nbr INT,
                                    sales FLOAT)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    location 'project/hive/warehouse/q4';

-- to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO q4_results
SELECT store_nbr, sales
FROM main_part;

INSERT OVERWRITE DIRECTORY 'project/output/q4'
    ROW FORMAT DELIMITED FIELDS
    TERMINATED BY ','
SELECT * FROM q4_results;