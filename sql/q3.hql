USE team7_projectdb;

DROP TABLE IF EXISTS q3_results;
CREATE EXTERNAL TABLE q3_results(
                                    dates DATE,
                                    type VARCHAR(20),
                                    locale VARCHAR(10),
                                    avg_sales FLOAT)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    location 'project/hive/warehouse/q3';

-- to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO q3_results
SELECT m.dates, h.type, h.locale, AVG(m.sales) AS avg_sales
FROM main_part as m
JOIN holidays_events as h ON m.dates == h.dates
GROUP BY m.dates, h.type, h.locale
ORDER BY avg_sales DESC;

INSERT OVERWRITE DIRECTORY 'project/output/q3'
    ROW FORMAT DELIMITED FIELDS
    TERMINATED BY ','
SELECT * FROM q3_results;