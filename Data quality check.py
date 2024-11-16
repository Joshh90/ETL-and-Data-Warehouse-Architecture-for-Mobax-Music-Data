"""
This script is used to manage the database schema for the Sonofy data warehouse.
It defines functions to drop existing tables and create new ones, ensuring the schema 
is correctly set up for the ETL process.

Functions:
    - drop_tables: Drops all tables in the database.
    - create_tables: Creates all required tables in the database.
    - main: Orchestrates the process of connecting to the database, dropping tables, 
      creating tables, and closing the connection.
"""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table listed in `drop_table_queries`.
    
    Parameters:
        cur (psycopg2.cursor): Cursor of the database connection.
        conn (psycopg2.connection): Database connection.

    Executes each drop table query from `sql_queries.py` and commits the transaction.
    """
    for query in drop_table_queries:
        print(f"Executing query: {query}")  # Debugging line
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table listed in `create_table_queries`.
    
    Parameters:
        cur (psycopg2.cursor): Cursor of the database connection.
        conn (psycopg2.connection): Database connection.

    Executes each create table query from `sql_queries.py` and commits the transaction.
    """
    for query in create_table_queries:
        print(f"Executing query: {query}")  # Debugging line
        cur.execute(query)
        conn.commit()


def main():
    """
    - Reads configuration from `dwh.cfg` to connect to the Redshift cluster.
    - Establishes a connection to the database and creates a cursor.
    - Drops all tables (if they exist) and creates new ones.
    - Closes the database connection after operations are complete.

    Usage:
        This script can be run as a standalone program by executing:
        `python create_tables.py`
    """
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Establish connection to Redshift
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    )
    cur = conn.cursor()

    # Drop and recreate tables
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # Close the connection
    conn.close()
    print("Connection closed.")


if __name__ == "__main__":
    main()
