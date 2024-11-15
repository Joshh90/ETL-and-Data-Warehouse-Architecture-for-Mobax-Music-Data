import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def copy_data(cur, conn):
    """
    Loads data from S3 into staging tables in Redshift using the COPY commands.
    """
    for query in copy_table_queries:
        print(f"Executing COPY command:\n\n{query}\n")
        cur.execute(query)
        conn.commit()


def insert_data(cur, conn):
    """
    Inserts data from staging tables into the analytical tables.
    """
    for query in insert_table_queries:
        print(f"Executing query:\n\n{query}\n")
        cur.execute(query)
        conn.commit()


def main():
    """
    Establishes a connection to the Redshift cluster, runs the ETL process to 
    load data from S3 to staging tables, and then populates the analytics tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to Redshift
    conn = psycopg2.connect(
        host=config['CLUSTER']['HOST'],
        dbname=config['CLUSTER']['DB_NAME'],
        user=config['CLUSTER']['DB_USER'],
        password=config['CLUSTER']['DB_PASSWORD'],
        port=config['CLUSTER']['DB_PORT']
    )
    cur = conn.cursor()

    print("Starting data loading from S3 to Redshift staging tables...")
    copy_data(cur, conn)
    
    print("Starting data insertion into final analytical tables...")
    insert_data(cur, conn)

    conn.close()
    print("ETL process completed and connection closed.")


if __name__ == "__main__":
    main()
