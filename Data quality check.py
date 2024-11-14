import psycopg2
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

# Define your SQL queries
songplay_table_insert = """
INSERT INTO songplays (user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
    e.userId AS user_id, 
    e.level, 
    s.song_id, 
    s.artist_id, 
    e.sessionId AS session_id, 
    e.location, 
    e.userAgent AS user_agent
FROM staging_events e
JOIN staging_songs s 
  ON e.song = s.title 
  AND e.artist = s.artist_name 
  AND e.length = s.duration
WHERE e.page = 'NextSong';
"""

# Function to insert data into songplays table
def insert_data(cur, conn):
    # Insert data into songplays table
    logging.info("Inserting data into songplays")
    cur.execute(songplay_table_insert)
    conn.commit()

# Main function
def main():
    try:
        # Connect to the PostgreSQL database using your credentials
        conn = psycopg2.connect(
            dbname="dwh",                  # Database name
            user="dwhuser",                # Username
            password="Passw0rd",           # Password
            host="dwhcluster.czyx5t8vbys1.us-east-1.redshift.amazonaws.com",  # Host
            port="5439"                    # Port (Redshift default port)
        )
        cur = conn.cursor()

        logging.info("Checking staging data...")
        
        # Example queries to check staging data
        cur.execute("SELECT COUNT(*) FROM staging_events;")
        logging.info(f"staging_events has {cur.fetchone()[0]} records")
        
        cur.execute("SELECT COUNT(*) FROM staging_songs;")
        logging.info(f"staging_songs has {cur.fetchone()[0]} records")
        
        # Step to insert data into songplays table
        logging.info("Inserting data into songplays")
        insert_data(cur, conn)
        
        logging.info("ETL process completed successfully!")

    except Exception as e:
        logging.error(f"ETL process failed: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    main()