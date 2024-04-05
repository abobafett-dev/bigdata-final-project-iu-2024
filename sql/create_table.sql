START TRANSACTION;


--stores
CREATE TABLE IF NOT EXISTS stores(
                                     store_nbr INTEGER NOT NULL PRIMARY KEY,
                                     city VARCHAR (20) NOT NULL,
                                     state VARCHAR (40) NOT NULL,
                                     type VARCHAR (1) NOT NULL,
                                     cluster INTEGER NOT NULL
);


--train data
CREATE TABLE IF NOT EXISTS main(
                                   id INTEGER NOT NULL PRIMARY KEY,
                                   "date" DATE NOT NULL,
                                   store_nbr INTEGER NOT NULL REFERENCES stores(store_nbr),
                                   family VARCHAR (30) NOT NULL,
                                   sales REAL NOT NULL,
                                   onpromotion INTEGER NOT NULL
);


--oil
CREATE TABLE IF NOT EXISTS oil(
                                  id SERIAL NOT NULL PRIMARY KEY,
                                  "date" DATE NOT NULL,
                                  dcoilwtico REAL
);


--transactions
CREATE TABLE IF NOT EXISTS transactions(
                                           id SERIAL NOT NULL PRIMARY KEY,
                                           date DATE NOT NULL,
                                           store_nbr INTEGER NOT NULL REFERENCES stores(store_nbr),
                                           transactions INTEGER NOT NULL
);


--holidays_events
CREATE TABLE IF NOT EXISTS holidays_events(
                                              id SERIAL NOT NULL PRIMARY KEY,
                                              date DATE NOT NULL,
                                              type VARCHAR (20) NOT NULL,
                                              locale VARCHAR (10) NOT NULL,
                                              locale_name VARCHAR (40) NOT NULL,
                                              description VARCHAR (50) NOT NULL,
                                              transferred BOOL NOT NULL
);


COMMIT;