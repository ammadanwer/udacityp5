songplay_table_insert = ("""
INSERT INTO public.songplays(start_time, userid, level, songid, artistid, sessionid, location, user_agent)
SELECT 
    TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second' AS start_time,
    e.userid,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionid,
    e.location,
    e.useragent    
FROM public.staging_events e
LEFT JOIN public.staging_songs s
ON e.song = s.title
AND e.artist = s.artist_name
WHERE page = 'NextSong';
""")

# using the window function here in the CTE
# enables us to filter and pick only the latest of the duplicate user entries
user_table_insert = ("""
INSERT INTO public.users(userid, first_name, last_name, gender, level)
WITH unique_user AS (
    SELECT userid,
    firstname,
    lastname,
    gender,
    level,
    ROW_NUMBER() over (partition by userid order by ts desc ) as index
FROM public.staging_events
)
SELECT userid,
    firstname AS first_name,
    lastname AS last_name,
    gender,
    level
FROM unique_user
WHERE COALESCE(userid, -1) <> -1 and unique_user.index = 1;
""")

song_table_insert = ("""
INSERT INTO public.songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,    
    duration
FROM public.staging_songs;
""")

artist_table_insert = ("""
INSERT INTO public.artists(artistid, name, location, lattitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,    
    artist_longitude
FROM public.staging_songs;
""")

time_table_insert = ("""
INSERT INTO public.time(start_time, hour, day, week, month, year, weekday)
WITH time_parse AS
(
    SELECT
        DISTINCT TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second' AS start_time
    FROM public.staging_events
)
SELECT
    start_time AS start_time,
    EXTRACT (hour from start_time) AS hour,
    EXTRACT (day from start_time) AS day,
    EXTRACT (week from start_time) AS week,
    EXTRACT (month from start_time) AS month,
    EXTRACT (year from start_time) AS year,
    EXTRACT (dow from start_time) AS weekday
FROM time_parse;
""")
