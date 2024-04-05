DELETE FROM main;
DELETE FROM transactions;
DELETE FROM stores;
DELETE FROM oil;
DELETE FROM holidays_events;

COPY stores FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '"';
COPY main FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '"';
COPY oil("date", dcoilwtico) FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '"' NULL AS '';
COPY transactions("date", store_nbr, transactions) FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '"';
COPY holidays_events("date", type, locale, locale_name, description, transferred) FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '"';