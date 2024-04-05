import psycopg2 as psql
from pprint import pprint
import os


# Read password from secrets file
file = os.path.join("secrets", ".psql.pass")
with open(file, "r") as file:
    password = file.read().rstrip()


# build connection string
conn_string = "host=hadoop-04.uni.innopolis.ru port=5432 user=team7 dbname=team7_projectdb password={}".format(password)


# Connect to the remote dbms
with psql.connect(conn_string) as conn:
    # Create a cursor for executing psql commands
    cur = conn.cursor()
    # Read the commands from the file and execute them.
    with open(os.path.join("sql", "create_table.sql")) as file:
        content = file.read()
        cur.execute(content)
    conn.commit()

    # Read the commands from the file and execute them.
    with open(os.path.join("sql", "import_data.sql")) as file:
        commands = file.readlines()
        cur.execute("".join(commands[0:5]))
        with open(os.path.join("data", "stores.csv"), "r") as stores:
            cur.copy_expert(commands[6], stores)
        with open(os.path.join("data", "train.csv"), "r") as main:
            cur.copy_expert(commands[7], main)
        with open(os.path.join("data", "oil.csv"), "r") as oil:
            cur.copy_expert(commands[8], oil)
        with open(os.path.join("data", "transactions.csv"), "r") as transactions:
            cur.copy_expert(commands[9], transactions)
        with open(os.path.join("data", "holidays_events.csv"), "r") as holidays_events:
            cur.copy_expert(commands[10], holidays_events)

    # If the sql statements are CRUD then you need to commit the change
    conn.commit()

    pprint(conn)
    cur = conn.cursor()
    # Read the sql commands from the file
    with open(os.path.join("sql", "test_database.sql")) as file:
        commands = file.readlines()
        for command in commands:
            cur.execute(command)
            # Read all records and print them
            pprint(cur.fetchall())
