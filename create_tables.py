import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    Drops each table listed in `drop_table_queries`.
    
    Parameters:
        cur (psycopg2.cursor): Cursor of the database connection
        conn (psycopg2.connection): Database connection
        
    Executes each drop table query and commits the transaction.
    """
    for query in drop_table_queries:
        print(f"Executing query: {query}")  # Debugging line
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Creates each table listed in `create_table_queries`.
    
    Parameters:
        cur (psycopg2.cursor): Cursor of the database connection
        conn (psycopg2.connection): Database connection

    Executes each create table query and commits the transaction.
    """
    for query in create_table_queries:
        print(f"Executing query: {query}")  # Debugging line
        cur.execute(query)
        conn.commit()

def main():
    """
    - Reads configuration from dwh.cfg to connect to the Redshift cluster.
    - Establishes a connection to the database and gets a cursor.
    - Drops all tables (if they exist) and creates new ones.
    - Closes the database connection after operations are complete.
    
    Usage:
        This script can be run as a standalone program by executing:
        `python create_tables.py`
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print("Connection closed.")

if __name__ == "__main__":
    main()
