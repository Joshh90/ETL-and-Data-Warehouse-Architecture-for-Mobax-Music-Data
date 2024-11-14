import psycopg2
import configparser

# Load configurations
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Extract credentials and connection info from the config file
HOST = config.get("CLUSTER", "HOST")
DB_NAME = config.get("CLUSTER", "DB_NAME")
DB_USER = config.get("CLUSTER", "DB_USER")
DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
DB_PORT = config.get("CLUSTER", "DB_PORT")

# Connect to the Redshift cluster using context manager for auto cleanup
try:
    with psycopg2.connect(
        host=HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    ) as conn:
        with conn.cursor() as cur:
            # Queries for validation checks
            count_queries = [
                ("artists", "SELECT COUNT(*) FROM artists;"),
                ("users", "SELECT COUNT(*) FROM users;"),
                ("songs", "SELECT COUNT(*) FROM songs;"),
                ("staging_songs", "SELECT COUNT(*) FROM staging_songs;")
            ]

            # Execute and print record counts for each table
            print("Record Counts:")
            for table_name, query in count_queries:
                try:
                    cur.execute(query)
                    result = cur.fetchone()
                    print(f"Table: {table_name}, Record Count: {result[0]}")
                except Exception as e:
                    print(f"Error querying {table_name}: {e}")

except Exception as e:
    print("Error connecting to the database or executing queries:", e)