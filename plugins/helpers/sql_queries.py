class SqlQueries:
    songplay_table_insert = ("""
        INSERT INTO songplays(playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        INSERT INTO users (userid, first_name, last_name, gender, level)
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        INSERT INTO songs (songid, title, artistid, year, duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        INSERT INTO artists (artistid, name, location, lattitude, longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        INSERT INTO "time" (start_time, "hour", "day", week, "month", "year", weekday)
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    songplay_count_test = ("""
        SELECT COUNT(*) FROM songplays;
    """)

    users_count_test = ("""
        SELECT COUNT(*) FROM users;
    """)

    songs_count_test = ("""
        SELECT COUNT(*) FROM songs;
    """)

    artists_count_test = ("""
        SELECT COUNT(*) FROM artists;
    """)

    time_count_test = ("""
        SELECT COUNT(*) FROM "time";
    """)

    users_null_test = ("""
        SELECT COUNT(*) FROM users WHERE COALESCE(userid, -1) = -1;
    """)

    songs_null_test = ("""
        SELECT COUNT(*) FROM songs WHERE COALESCE(songid, '') = '';
    """)

    artists_null_test = ("""
        SELECT COUNT(*) FROM artists WHERE COALESCE(artistid, '') = '';
    """)

    time_null_test = ("""
        SELECT COUNT(*) FROM "time" WHERE COALESCE(start_time::text, '') = '';
    """)