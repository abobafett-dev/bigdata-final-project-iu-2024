DELETE FROM main;
DELETE FROM transactions;
DELETE FROM stores;
DELETE FROM oil;
DELETE FROM holidays_events;

COPY stores FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '"';
COPY main FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '"';
COPY oil(dates, dcoilwtico) FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '"' NULL AS '';
COPY transactions(dates, store_nbr, transactions) FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '"';
COPY holidays_events(dates, type, locale, locale_name, description, transferred) FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '"';