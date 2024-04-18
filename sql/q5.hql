USE teamx_projectdb;

DROP TABLE IF EXISTS q1_results;
CREATE EXTERNAL TABLE q1_results(
                                    Dname STRING,
                                    Total_Salaries FLOAT)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    location 'project/hive/warehouse/q1';

-- to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO q1_results
SELECT dname,
       SUM(sal) AS total_sal
FROM departments AS d
         JOIN employees AS e ON d.deptno = e.deptno
GROUP BY dname
ORDER BY total_sal DESC
LIMIT 10;

SELECT * FROM q1_results;