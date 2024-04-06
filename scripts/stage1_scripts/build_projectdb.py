"""Python script for autonomous SQL query execution for stage 1"""

import os
from pprint import pprint
import psycopg2 as psql


# Read password from secrets file
file = os.path.join("secrets", ".psql.pass")
with open(file, "r", encoding="UTF-8") as file:
    password = file.read().rstrip()


# build connection string
conn_string = f"host=hadoop-04.uni.innopolis.ru port=5432 user=team7\
dbname=team7_projectdb password={password}"


# Connect to the remote dbms
with psql.connect(conn_string) as conn:
    # Create a cursor for executing psql commands
    cur = conn.cursor()
    # Read the commands from the file and execute them.
    with open(os.path.join("sql", "create_table.sql"), "r", encoding="UTF-8") as file:
        content = file.read()
        cur.execute(content)
    conn.commit()

    # Read the commands from the file and execute them.
    with open(os.path.join("sql", "import_data.sql"), "r", encoding="UTF-8") as file:
        commands = file.readlines()
        cur.execute("".join(commands[0:5]))
        with open(os.path.join("data", "stores.csv"), "r", encoding="UTF-8") as stores:
            cur.copy_expert(commands[6], stores)
        with open(os.path.join("data", "train.csv"), "r", encoding="UTF-8") as main:
            cur.copy_expert(commands[7], main)
        with open(os.path.join("data", "oil.csv"), "r", encoding="UTF-8") as oil:
            cur.copy_expert(commands[8], oil)
        with open(os.path.join("data", "transactions.csv"), "r", encoding="UTF-8") as transactions:
            cur.copy_expert(commands[9], transactions)
        hol_path = os.path.join("data", "holidays_events.csv")
        with open(hol_path, "r", encoding="UTF-8") as holidays_events:
            cur.copy_expert(commands[10], holidays_events)

    # If the sql statements are CRUD then you need to commit the changec
    conn.commit()

    pprint(conn)
    cur = conn.cursor()
    # Read the sql commands from the file
    with open(os.path.join("sql", "test_database.sql"), "r", encoding="UTF-8") as file:
        commands = file.readlines()
        for command in commands:
            cur.execute(command)
            # Read all records and print them
            pprint(cur.fetchall())
