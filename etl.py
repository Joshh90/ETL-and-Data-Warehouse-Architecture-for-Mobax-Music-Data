import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, copy_table_queries, insert_table_queries

# CONFIGURATION
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DATABASE CONNECTION
def create_connection():
    return psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))

def create_cursor(conn):
    return conn.cursor()

def drop_tables(cur, conn):
    for query in drop_table_queries:
        print(f"Executing query: {query}")
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    for query in create_table_queries:
        print(f"Executing query: {query}")
        cur.execute(query)
        conn.commit()

def copy_data(cur, conn):
    for query in copy_table_queries:
        print(f"Executing query: {query}")
        cur.execute(query)
        conn.commit()

def insert_data(cur, conn):
    for query in insert_table_queries:
        print(f"Executing query: {query}")
        cur.execute(query)
        conn.commit()

def full_table_update(cur, conn):
    # Step 1: Truncate all tables before inserting new data
    truncate_queries = [
        "TRUNCATE TABLE songplays;",
        "TRUNCATE TABLE users;",
        "TRUNCATE TABLE songs;",
        "TRUNCATE TABLE artists;",
        "TRUNCATE TABLE time;"
    ]
    for query in truncate_queries:
        print(f"Executing query: {query}")
        cur.execute(query)
        conn.commit()

    # Step 2: Insert updated data
    insert_data(cur, conn)

def main():
    # Establish the database connection and cursor
    conn = create_connection()
    cur = create_cursor(conn)
    
    # Drop and create tables
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    # Load data into staging tables
    copy_data(cur, conn)
    
    # Perform the full table update (truncate and reload data)
    full_table_update(cur, conn)
    
    # Close the connection
    conn.close()

if __name__ == "__main__":
    main()