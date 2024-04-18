DROP DATABASE IF EXISTS team7_projectdb CASCADE;
CREATE DATABASE team7_projectdb LOCATION "project/hive/warehouse";
USE team7_projectdb;

CREATE EXTERNAL TABLE stores STORED AS AVRO LOCATION 'project/warehouse/stores' TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/stores.avsc');

CREATE EXTERNAL TABLE main STORED AS AVRO LOCATION 'project/warehouse/main' TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/main.avsc');

CREATE EXTERNAL TABLE oil STORED AS AVRO LOCATION 'project/warehouse/oil' TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/oil.avsc');
CREATE TABLE oil_dates AS SELECT ID, to_date(from_unixtime(FLOOR(CAST(dates AS BIGINT)/1000), 'yyyy-MM-dd HH:mm:ss.SSS')) as dates, dcoilwtico FROM oil;
DROP TABLE oil;
ALTER TABLE oil_dates RENAME TO oil;

CREATE EXTERNAL TABLE transactions STORED AS AVRO LOCATION 'project/warehouse/transactions' TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/transactions.avsc');
CREATE TABLE transactions_dates AS SELECT ID, to_date(from_unixtime(FLOOR(CAST(dates AS BIGINT)/1000), 'yyyy-MM-dd HH:mm:ss.SSS')) as dates, store_nbr, transactions FROM transactions;
DROP TABLE transactions;
ALTER TABLE transactions_dates RENAME TO transactions;

CREATE EXTERNAL TABLE holidays_events STORED AS AVRO LOCATION 'project/warehouse/holidays_events' TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/holidays_events.avsc');
CREATE TABLE holidays_events_dates AS SELECT ID, to_date(from_unixtime(FLOOR(CAST(dates AS BIGINT)/1000), 'yyyy-MM-dd HH:mm:ss.SSS')) as dates, type, locale, locale_name, description, transferred FROM holidays_events;
DROP TABLE holidays_events;
ALTER TABLE holidays_events_dates RENAME TO holidays_events;

SELECT * FROM main LIMIT 10;
SELECT * FROM stores LIMIT 10;
SELECT * FROM oil LIMIT 10;
SELECT * FROM transactions LIMIT 10;
SELECT * FROM holidays_events LIMIT 10;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE EXTERNAL TABLE IF NOT EXISTS main_part(
    ID int,
    dates DATE,
    family varchar(30),
    sales float,
    onpromotion int)
PARTITIONED BY (store_nbr int)
STORED AS AVRO LOCATION 'project/hive/warehouse/main_part'
TBLPROPERTIES ('AVRO.COMPRESS'='SNAPPY');

INSERT INTO main_part
PARTITION (store_nbr)
SELECT ID, to_date(from_unixtime(FLOOR(CAST(dates AS BIGINT)/1000), 'yyyy-MM-dd HH:mm:ss.SSS')) as dates, family, sales, onpromotion, store_nbr
FROM main;

DROP TABLE main;