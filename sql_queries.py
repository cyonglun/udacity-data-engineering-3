import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE_ARN= config.get('IAM_ROLE', 'ARN')
LOG_DATA= config.get('S3', 'LOG_DATA')
SONG_DATA= config.get('S3', 'SONG_DATA')
LOG_JSONPATH= config.get('S3', 'LOG_JSONPATH')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist VARCHAR,
        auth VARCHAR, 
        firstName VARCHAR,
        gender VARCHAR,   
        itemInSession INTEGER,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR, 
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts TIMESTAMP,
        userAgent VARCHAR,
        userId INTEGER
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR, 
        artist_name VARCHAR, 
        song_id VARCHAR, 
        title VARCHAR, 
        duration FLOAT, 
        year INTEGER
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, 
        start_time TIMESTAMP, 
        user_id VARCHAR, 
        level VARCHAR, 
        song_id VARCHAR, 
        artist_id VARCHAR, 
        session_id INTEGER, 
        location VARCHAR sortkey, 
        user_agent VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id INTEGER PRIMARY KEY distkey, 
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender VARCHAR, 
        level VARCHAR
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR PRIMARY KEY distkey, 
        title VARCHAR, 
        artist_id VARCHAR, 
        year INTEGER, 
        duration FLOAT
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR PRIMARY KEY distkey, 
        name VARCHAR, 
        location VARCHAR, 
        latitude FLOAT, 
        longitude FLOAT
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP PRIMARY KEY sortkey distkey, 
        hour INTEGER, 
        day INTEGER, 
        week INTEGER, 
        month INTEGER,
        year INTEGER, 
        weekday INTEGER
    )
""")

# STAGING TABLES
staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    timeformat 'epochmillisecs'
    json {};
""").format(LOG_DATA, IAM_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    json 'auto';
""").format(SONG_DATA, IAM_ROLE_ARN)

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays(
        start_time, 
        user_id, 
        level, 
        song_id, 
        artist_id, 
        session_id, 
        location, 
        user_agent) 
    SELECT DISTINCT
        se.ts AS start_time, 
        se.userId AS user_id, 
        se.level, 
        ss.song_id, 
        ss.artist_id, 
        se.sessionId AS session_id, 
        se.location, 
        se.userAgent AS user_agent
    FROM staging_events se, staging_songs ss
    WHERE se.page = 'NextSong'
    AND se.song = ss.title
    AND se.artist = ss.artist_name
    AND se.length = ss.duration
""")

user_table_insert = ("""
    INSERT INTO users(
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level) 
    SELECT
        se.userId AS user_id,
        se.firstName AS first_name,
        se.lastName AS last_name,
        se.gender,
        se.level
    FROM staging_events se
    WHERE se.page = 'NextSong'
    GROUP BY se.userId, se.firstName, se.lastName, se.gender, se.level
    ORDER BY se.userId ASC
""")

song_table_insert = ("""
    INSERT INTO songs(
        song_id, 
        title, 
        artist_id, 
        year, 
        duration) 
    SELECT DISTINCT
        ss.song_id,
        ss.title,
        ss.artist_id,
        ss.year,
        ss.duration
    FROM staging_songs ss
    WHERE ss.song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists(
        artist_id, 
        name, 
        location, 
        latitude, 
        longitude) 
    SELECT DISTINCT
        ss.artist_id,
        ss.artist_name AS name,
        ss.artist_location AS location,
        ss.artist_latitude AS latitude,
        ss.artist_longitude AS longitude
    FROM staging_songs ss
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time(
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday) 
    SELECT
        se.ts AS start_time,
        EXTRACT(hour FROM start_time) AS hour,
        EXTRACT(day FROM start_time) AS day,
        EXTRACT(week FROM start_time) AS week,
        EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year,
        EXTRACT(dayofweek FROM start_time) AS weekday
    FROM (
        SELECT ts 
        FROM staging_events
        GROUP BY ts
        ) AS se
    ORDER BY se.ts ASC
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
