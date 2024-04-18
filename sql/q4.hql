USE team7_projectdb;

DROP TABLE IF EXISTS q4_results;
CREATE EXTERNAL TABLE q4_results(
                                    dates DATE,
                                    type VARCHAR(20),
                                    locale VARCHAR(10),
                                    avg_sales FLOAT)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    location 'project/hive/warehouse/q4';

-- to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO q4_results
SELECT m.dates, h.type, h.locale, AVG(m.sales) AS avg_sales
FROM main_part as m
         JOIN holidays_events as h ON m.dates == h.dates
GROUP BY m.dates, h.type, h.locale
ORDER BY avg_sales DESC;

INSERT OVERWRITE DIRECTORY 'project/output/q4'
    ROW FORMAT DELIMITED FIELDS
    TERMINATED BY ','
SELECT * FROM q4_results;