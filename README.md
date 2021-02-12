# Data Warehouse

This repo contains my solution for the Data Warehouse project of the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

## Purpose

The project involves an imaginary music streaming startup called Sparkify. Sparkify is interested in analytical insights into the streaming habits of the users. Sparkify has song metadata as well as user activity logs, both as JSON files stored in S3. Data is a subset of the [Million Song Dataset](http://millionsongdataset.com/) [1].

To provide analytics we need to setup a cloud data warehouse using Amazon Redshift. To populate the database we need to create an ETL pipeline that imports the data from S3 into staging tables in Redshift, and then transforms the staging data into fact and dimension tables.

## Database schema design

### Staging tables

For staging the data two simple tables that match the schema of the input files will be used:

**staging_events**

| Column                    | Data type/constaints  |
| ------------------------- | --------------------- |
| artist                    | VARCHAR(255)          |
| auth                      | VARCHAR(25) NOT NULL  |
| first_name                | VARCHAR(50)           |
| gender                    | CHAR(1)               |
| item_in_session           | INTEGER NOT NULL      |
| last_name                 | VARCHAR(50)           |
| length                    | DECIMAL(18,8)         |
| level                     | VARCHAR(25) NOT NULL  |
| location                  | VARCHAR(255)          |
| method                    | VARCHAR(10) NOT NULL  |
| page                      | VARCHAR(100) NOT NULL |
| registration              | FLOAT                 |
| session_id                | INTEGER NOT NULL      |
| song                      | VARCHAR(255)          |
| status                    | INTEGER NOT NULL      |
| ts                        | BIGINT NOT NULL       |
| user_agent                | VARCHAR(255)          |
| user_id                   | INTEGER               |

**staging_songs**

| Column                    | Data type/constraints  |
| ------------------------- | ---------------------- |
| num_songs                 | INTEGER NOT NULL       |
| artist_id                 | VARCHAR(25) NOT NULL   |
| artist_latitude           | DECIMAL(11,8)          |
| artist_longitude          | DECIMAL(11,8)          |
| artist_location           | VARCHAR(255)           |
| artist_name               | VARCHAR(255) NOT NULL  |
| song_id                   | VARCHAR(25) NOT NULL   |
| title                     | VARCHAR(255) NOT NULL  |
| duration                  | DECIMAL(18,8) NOT NULL |
| year                      | INTEGER NOT NUL        |

### Fact and dimension tables

For analysis purposes, fact and dimension tables are created and populated using data from the staging tables.

The fact table will contain song plays, since this is what we want to query and aggregate:

**songplays (DISTSTYLE KEY)**

| Column      | Data type/constraints             |
| ----------- | --------------------------------- |
| songplay_id | INTEGER IDENTITY(0,1) PRIMARY KEY |
| start_time  | TIMESTAMP NOT NULL SORTKEY        |
| user_id     | INTEGER NOT NULL                  |
| level       | VARCHAR(25) NOT NULL              |
| song_id     | VARCHAR(25) DISTKEY               |
| artist_id   | VARCHAR(25)                       |
| session_id  | INTEGER NOT NULL                  |
| location    | VARCHAR(255)                      |
| user_agent  | VARCHAR(255)                      |

Dimension tables represent the three types of objects (users, songs and artists) that are relevant for filtering and grouping data, as well as a time table for filtering by various time components:

**users (DISTSTYLE ALL)**

| Column     | Data type/constraints                |
| ---------- | ------------------------------------ |
| user_id    | INTEGER NOT NULL PRIMARY KEY SORTKEY |
| first_name | VARCHAR(50)                          |
| last_name  | VARCHAR(50)                          |
| gender     | CHAR(1)                              |
| level      | VARCHAR(25) NOT NULL                 |

**songs (DISTSTYLE KEY)**

| Column    | Data type/constraints                    |
| --------- | ---------------------------------------- |
| song_id   | VARCHAR(25) NOT NULL PRIMARY KEY DISTKEY |
| title     | VARCHAR(255) NOT NULL                    |
| artist_id | VARCHAR(25) NOT NULL                     |
| year      | INTEGER NOT NULL                         |
| duration  | DECIMAL(18,8) NOT NULL                   |

**artists (DISTSTYLE ALL)**

| Column    | Data type/constraints                    |
| --------- | ---------------------------------------- |
| artist_id | VARCHAR(25) NOT NULL PRIMARY KEY SORTKEY |
| name      | VARCHAR(255) NOT NULL                    |
| location  | VARCHAR(255)                             |
| latitude  | DECIMAL(11,8)                            |
| longitude | DECIMAL(11,8)                            |

**time**

| Column     | Data type/constraints                  |
| ---------- | -------------------------------------- |
| start_time | TIMESTAMP NOT NULL PRIMARY KEY SORTKEY |
| hour       | SMALLINT NOT NULL                      |
| day        | SMALLINT NOT NULL                      |
| week       | SMALLINT NOT NULL                      |
| month      | SMALLINT NOT NULL                      |
| year       | SMALLINT NOT NULL                      |
| weekday    | SMALLINT NOT NULL                      |

### Choosing diststyle, distkeys and sortkeys

When choosing how to distribute data among the Redshift nodes, I make the following observations:

* The fact table `songplays` will most likely be the largest table (maybe not in the limited test data set, but in the real world).
* The `time` table will be similar size as `songplays` since it has a row for every timestamp.
* The `songs` and `users` tables might be large, but not as large as `songplays`.
* The `artist` table will be smaller than `songs`, since artists produce multiple songs.
* Queries will often be limited in time, for example to look at streaming activity within the last day or month.
* The `songs` dimension is probably one of the most typical to join with the fact table, besides `time`.

Based on these observations I will distribute data as follows:

* `songplays` and `songs` will use diststyle `KEY` with `song_id` as `DISTKEY`. This way songs will be collocated with the songplays and optimized for song joins.
* Although the large `time` table is a candidate for collocation with `songplays`, it would skew the query execution if we often filter for a narrow date period, so I will not set `songplays.start_time` as `DISTKEY`. Instead I will set a `SORTKEY` on `start_time` in both `songplays` and `time` for quick timestamp scanning.
* It is unclear whether `artist` and `users` tables will fit into each node. I will assume that they do, and set them to distyle `ALL`, meaning all artist and user data will be replicated across all Redshift nodes. Since we join by the ID fields I will set `users.user_id` and `artist.artist_id` as `SORTKEY`.

## ETL pipeline

Each step in the pipeline involves access to the Redshift database. For this purpose I have moved all database connection handling into a `DbClient` Python class.

### Staging data

The first step of the pipeline is importing data from the two sets of JSON files into staging tables. This is done using two `COPY` SQL statements that lets Redshift fetch the data directly from the S3 buckets. For the log data we specify a JSON path file to help point to the correct JSON fields. For the song data this is not necessary since the database columns name match the JSON fields directly.

### Analytics data

The second step of the ETL pipeline is importing data from the staging tables into the fact and dimension tables.

* **Song plays** are imported from the event table. Unfortunately there are no song IDs or artist IDs in the event table but only song titles and artist names. For this reason we have to `JOIN` into the song table to find a matching song using the song title and artist name. I use a `LEFT JOIN` and leave `song_id` and `artist_id` empty in the fact table for missing songs. In the same process the integer timestamp from the staging table is converted into a Redshift `TIMESTAMP`. This is possible using the `DATE_ADD` SQL function and the reference date of `1970/01/01` along with the given timestamp which represents milliseconds since the reference date.
* **Users** are imported from the event table, ignoring duplicates.
* **Songs** are imported directly from the song staging table.
* **Artists** are imported from the song staging table, ignoring duplicates.
* **Time** table is imported using the timestamps from the event staging table, and using SQL functions to extract time components (hour, day, week etc.).

## Example queries

Run `python analysis.py` to execute three example analysis queries with the following results:

Most popular songs:

| song_title                                           | artist_name   | count_plays |
| ---------------------------------------------------- | ------------- | ----------- |
| You're The One                                       | Dwight Yoakam |          37 |
| Catch You Baby (Steve Pitron & Max Sanna Radio Edit) | Lonnie Gordon |           9 |
| I CAN'T GET STARTED                                  | Ron Carter    |           9 |
| Nothin' On You [feat. Bruno Mars] (Album Version)    | B.o.B         |           8 |
| Hey Daddy (Daddy's Home)                             | Usher         |           6 |

Number of song plays by hour of the day and subscription level:

| hours | level | count_plays |
| ----- | ----- | ----------- |
|     0 | free  |          34 |
|     0 | paid  |         121 |
|     1 | free  |          26 |
|     1 | paid  |         128 |
|     2 | free  |          31 |
|     2 | paid  |          86 |
|   ... | ...   |         ... |

Number of song plays per user, for paid male/female and free male/female users:

| level | gender | plays_per_user   |
| ----- | ------ | ---------------- |
| paid  | M      | 185.285714285714 |
| free  | M      | 17.1891891891892 |
| free  | F      | 13.1777777777778 |
| paid  | F      | 286.266666666667 |

## How to run

The code requires a Python environment to run, and the `psycopg2` package installed.

Additionally, an Amazon Redshift cluster must be available, and credentials must be configured in `dwh.cfg`. Please create this file based on `dwh.cfg.example`.

Run the following scripts, to create the database tables, run the ETL pipeline, and execute analysis queries:

```
python3 create_tables.py
python3 etl.py
python3 analysis.py
```

## References

[1] Thierry Bertin-Mahieux, Daniel P.W. Ellis, Brian Whitman, and Paul Lamere. The Million Song Dataset. In Proceedings of the 12th International Society for Music Information Retrieval Conference (ISMIR 2011), 2011.