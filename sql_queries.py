# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplays_table_drop = "DROP TABLE IF EXISTS songplays;"
users_table_drop = "DROP TABLE IF EXISTS users;"
songs_table_drop = "DROP TABLE IF EXISTS songs;"
artists_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession INT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    userAgent VARCHAR,
    userId INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INT
);
""")

users_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR
);
""")

songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INT,
    duration FLOAT
);
""")

artists_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);
""")

songplays_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(1,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR
);
""")

# COPY DATA

staging_events_copy = ("""
COPY staging_events 
FROM 's3://udacity-dend/log_data'
CREDENTIALS 'aws_iam_role=arn:aws:iam::284347131770:role/dwhRole'
REGION 'us-west-2'
JSON 's3://udacity-dend/log_json_path.json';
""")

staging_songs_copy = ("""
COPY staging_songs
FROM 's3://udacity-dend/song_data/A/A/A'
CREDENTIALS 'aws_iam_role=arn:aws:iam::284347131770:role/dwhRole'
REGION 'us-west-2'
JSON 'auto'
TRUNCATECOLUMNS
BLANKSASNULL
EMPTYASNULL;
""")

# INSERT DATA

songplays_table_insert = ("""
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
""")

users_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    e.userId AS user_id,
    e.firstName AS first_name,
    e.lastName AS last_name,
    e.gender,
    e.level
FROM staging_events e
WHERE e.userId IS NOT NULL;
""")

songs_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    s.song_id,
    s.title,
    s.artist_id,
    s.year,
    s.duration
FROM staging_songs s;
""")

artists_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    s.artist_id,
    s.artist_name AS name,
    s.artist_location AS location,
    s.artist_latitude AS latitude,
    s.artist_longitude AS longitude
FROM staging_songs s;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' AS start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(weekday FROM start_time) AS weekday
FROM staging_events e;
""")

# LIST OF QUERIES

create_table_queries = [staging_events_table_create, staging_songs_table_create, users_table_create, 
                        songs_table_create, artists_table_create, time_table_create, songplays_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplays_table_drop, 
                      users_table_drop, songs_table_drop, artists_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplays_table_insert, users_table_insert, songs_table_insert, 
                        artists_table_insert, time_table_insert]