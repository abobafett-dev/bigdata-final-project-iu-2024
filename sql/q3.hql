USE team7_projectdb;

DROP TABLE IF EXISTS q3_results;
CREATE EXTERNAL TABLE q3_results(
                                    is_holiday VARCHAR(10),
                                    sales FLOAT)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    location 'project/hive/warehouse/q3';

-- to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

WITH holiday_dates AS (
    SELECT dates
    FROM holidays_events
    WHERE type = 'Holiday' AND transferred = FALSE
),
     sales_with_holiday_flag AS (
         SELECT
             main_part.sales,
             CASE
                 WHEN holiday_dates.dates IS NOT NULL THEN 'Holiday'
                 ELSE 'Workday'
                 END AS is_holiday
         FROM main_part
                  LEFT JOIN holiday_dates ON main_part.dates = holiday_dates.dates
     )
INSERT INTO q3_results
SELECT
    is_holiday,
    sales
FROM sales_with_holiday_flag;

INSERT OVERWRITE DIRECTORY 'project/output/q3'
    ROW FORMAT DELIMITED FIELDS
    TERMINATED BY ','
SELECT * FROM q3_results;