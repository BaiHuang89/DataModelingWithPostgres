# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;" # user is a reserved word in PostgreSQL
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id BIGSERIAL PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INT NOT NULL,
        level VARCHAR(10) NOT NULL,
        song_id VARCHAR(100) NOT NULL,
        artist_id VARCHAR(100) NOT NULL,
        session_id INT NOT NULL,
        location VARCHAR(256),
        user_agent VARCHAR
    );

    COMMENT ON COLUMN songplays.songplay_id is 'AutoIncrement Column';
    COMMENT ON COLUMN songplays.user_agent is 'No Specified limit for Headers according to HTTP specification. Web servers do have the limit and vary.';
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY,
        first_name VARCHAR(100) NOT NULL,
        last_name VARCHAR(100) NOT NULL,
        gender VARCHAR(1) NOT NULL,
        level VARCHAR(20) NOT NULL
    );
""")

user_temp_table_create = ("""
    CREATE TABLE IF NOT EXISTS users_temp (
        LIKE users
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(100) PRIMARY KEY,
        title VARCHAR(256) NOT NULL,
        artist_id VARCHAR(100) NOT NULL,
        year SMALLINT,
        duration NUMERIC
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(100) PRIMARY KEY,
        name VARCHAR(256) NOT NULL,
        location VARCHAR(256),
        latitude NUMERIC,
        longitude NUMERIC
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY,
        hour SMALLINT NOT NULL,
        day SMALLINT NOT NULL,
        week SMALLINT NOT NULL,
        month SMALLINT NOT NULL,
        year SMALLINT NOT NULL,
        weekday SMALLINT NOT NULL
    );

    COMMENT ON COLUMN time.start_time is 'The value of log.ts';
""")

time_temp_table_create = ("""
    CREATE TABLE IF NOT EXISTS time_temp (
        LIKE time
    );
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT (user_id)
    DO NOTHING;
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration) 
    VALUES(%s, %s, %s, %s, %s) 
    ON CONFLICT (song_id) 
    DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude) 
    VALUES(%s, %s, %s, %s, %s) 
    ON CONFLICT (artist_id) 
    DO NOTHING;
""")


time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday) 
    VALUES(%s, %s, %s, %s, %s, %s, %s) 
    ON CONFLICT (start_time) 
    DO NOTHING;
""")


# FIND SONGS

song_select = ("""
    SELECT songs.song_id, artists.artist_id 
    FROM (songs JOIN artists ON songs.artist_id = artists.artist_id)
    WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s;
""")

# COPY RECORDS FROM TEMP TABLE
time_migrate = ("""
    INSERT INTO time
    SELECT DISTINCT ON(start_time) *
    FROM time_temp
    ORDER BY start_time
    ON CONFLICT (start_time) 
    DO NOTHING;

    DROP TABLE IF EXISTS time_temp;
""")

users_migrate = ("""
    INSERT INTO users
    SELECT DISTINCT ON(user_id) *
    FROM users_temp
    ORDER BY user_id
    ON CONFLICT (user_id) 
    DO NOTHING;

    DROP TABLE IF EXISTS users_temp;
""")

# Analysis user activities for listening music
analysis_most_popular_song = ("""
    SELECT songs.title AS "Song Title", artists.name AS "Artist Name"
    FROM ( songs JOIN artists ON songs.artist_id = artists.artist_id )
    WHERE songs.song_id = (
        SELECT songplays.song_id
        FROM ( songplays JOIN time ON songplays.start_time = time.start_time )
        WHERE time.year = %s
        GROUP BY songplays.song_id
        ORDER BY COUNT(songplays.song_id) DESC
        LIMIT 1
    );
""")

analysis_most_popular_artist = ("""
    SELECT artists.name AS "Artist Name", artists.location AS "Location"
    FROM artists
    WHERE artists.artist_id = (
        SELECT songplays.artist_id
        FROM ( songplays JOIN time ON songplays.start_time = time.start_time )
        WHERE time.year = %s
        GROUP BY songplays.artist_id
        ORDER BY COUNT(songplays.artist_id) DESC
        LIMIT 1
    );
""")

analysis_mean_number_on_different_level = ("""
    SELECT users.level AS "User Level", CAST(AVG(total.count) AS DECIMAL(10,2)) as "Avarage Number of Songs"
    FROM users 
    JOIN 
        (
            SELECT songplays.user_id AS id, COUNT(songplays.user_id) AS count
            FROM ( songplays JOIN time ON songplays.start_time = time.start_time )
            WHERE time.year = %s
            GROUP BY songplays.user_id
            ORDER BY count DESC
        ) AS total
    ON users.user_id = total.id
    GROUP BY users.level;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
migrate_from_temp_table = [time_migrate, users_migrate]