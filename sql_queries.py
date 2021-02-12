import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events
(
    artist          VARCHAR(255),
    auth            VARCHAR(25) NOT NULL,
    first_name      VARCHAR(50),
    gender          CHAR(1),
    item_in_session INTEGER NOT NULL,
    last_name       VARCHAR(50),
    length          DECIMAL(18,8),
    level           VARCHAR(25) NOT NULL,
    location        VARCHAR(255),
    method          VARCHAR(10) NOT NULL,
    page            VARCHAR(100) NOT NULL,
    registration    FLOAT,
    session_id      INTEGER NOT NULL,
    song            VARCHAR(255),
    status          INTEGER NOT NULL,
    ts              BIGINT NOT NULL,
    user_agent      VARCHAR(255),
    user_id         INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs
(
    num_songs           INTEGER NOT NULL,
    artist_id           VARCHAR(25) NOT NULL,
    artist_latitude     DECIMAL(11,8),
    artist_longitude    DECIMAL(11,8),
    artist_location     VARCHAR(255),
    artist_name         VARCHAR(255) NOT NULL,
    song_id             VARCHAR(25) NOT NULL,
    title               VARCHAR(255) NOT NULL,
    duration            DECIMAL(18,8) NOT NULL,
    year                INTEGER NOT NULL
);
""")

songplay_table_create = ("""
CREATE TABLE songplays
(
    songplay_id     INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time      TIMESTAMP NOT NULL SORTKEY,
    user_id         INTEGER NOT NULL,
    level           VARCHAR(25) NOT NULL,
    song_id         VARCHAR(25) DISTKEY,
    artist_id       VARCHAR(25),
    session_id      INTEGER NOT NULL,
    location        VARCHAR(255),
    user_agent      VARCHAR(255)
) DISTSTYLE KEY;
""")

user_table_create = ("""
CREATE TABLE users
(
    user_id     INTEGER NOT NULL PRIMARY KEY SORTKEY,
    first_name  VARCHAR(50),
    last_name   VARCHAR(50),
    gender      CHAR(1),
    level       VARCHAR(25) NOT NULL
) DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE songs
(
    song_id         VARCHAR(25) NOT NULL PRIMARY KEY DISTKEY,
    title           VARCHAR(255) NOT NULL,
    artist_id       VARCHAR(25) NOT NULL,
    year            INTEGER NOT NULL,
    duration        DECIMAL(18,8) NOT NULL
) DISTSTYLE KEY;
""")

artist_table_create = ("""
CREATE TABLE artists
(
    artist_id       VARCHAR(25) NOT NULL PRIMARY KEY SORTKEY,
    name            VARCHAR(255) NOT NULL,
    location        VARCHAR(255),
    latitude        DECIMAL(11,8),
    longitude       DECIMAL(11,8)
) DISTSTYLE ALL;
""")

time_table_create = ("""
CREATE TABLE time
(
    start_time  TIMESTAMP NOT NULL PRIMARY KEY SORTKEY,
    hour        SMALLINT NOT NULL,
    day         SMALLINT NOT NULL,
    week        SMALLINT NOT NULL,
    month       SMALLINT NOT NULL,
    year        SMALLINT NOT NULL,
    weekday     SMALLINT NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    json {} region 'us-west-2'
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    json 'auto' region 'us-west-2'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DATE_ADD('ms', e.ts, '1970-01-01'), e.user_id, e.level, s.song_id, s.artist_id, e.session_id, e.location, e.user_agent
FROM staging_events e
LEFT JOIN staging_songs s ON s.artist_name = e.artist AND s.title = e.song
WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id, first_name, last_name, gender, level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT  DISTINCT DATE_ADD('ms', e.ts, '1970-01-01') as start_time,
        EXTRACT(hour FROM start_time),
        EXTRACT(day FROM start_time),
        EXTRACT(week FROM start_time),
        EXTRACT(month FROM start_time),
        EXTRACT(year FROM start_time),
        EXTRACT(weekday FROM start_time)
FROM staging_events e
WHERE e.page = 'NextSong'
""")

# ANALYTICS QUERIES

popular_songs = ("""
SELECT s.title as song_title, a.name as artist_name, COUNT(*) as count_plays
FROM songplays p
INNER JOIN songs s ON s.song_id = p.song_id
INNER JOIN artists a ON a.artist_id = p.artist_id
GROUP BY s.title, a.name
ORDER BY count_plays DESC
LIMIT 5
""")

songplays_by_hour_and_level = ("""
SELECT t.hour, p.level, COUNT(*) as count_plays
FROM songplays p
INNER JOIN time t ON t.start_time = p.start_time
GROUP BY t.hour, p.level
ORDER BY t.hour, p.level
""")

songplays_per_user_for_level_and_gender = ("""
SELECT p.level, u.gender, CAST (COUNT(*) AS float) / COUNT(DISTINCT u.user_id) as plays_per_user
FROM songplays p
INNER JOIN users u ON u.user_id = p.user_id
GROUP BY p.level, u.gender
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
analysis_queries = [popular_songs, songplays_by_hour_and_level, songplays_per_user_for_level_and_gender]
