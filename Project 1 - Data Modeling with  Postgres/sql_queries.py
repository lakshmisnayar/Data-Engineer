# DROP TABLES

songplay_table_drop = "DROP TABLE songplays"
user_table_drop = "DROP TABLE users"
song_table_drop = "DROP TABLE songs"
artist_table_drop = "DROP TABLE artists"
time_table_drop = "DROP TABLE time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE songplays (
                                           songplay_id  SERIAL, 
                                           start_time timestamp  REFERENCES time, 
                                           user_id text REFERENCES users, 
                                           level text,  
                                           song_id text REFERENCES songs,
                                           artist_id text REFERENCES artists,
                                           session_id integer, 
                                           location text, 
                                           user_agent text, 
                                           PRIMARY KEY(songplay_id)
                                           )""")

user_table_create = ("""CREATE TABLE users (  
                                          user_id text NOT NULL, 
                                          first_name text NOT NULL,
                                          last_name text NOT NULL, 
                                          gender text NOT NULL, 
                                          level text NOT NULL, 
                                          PRIMARY KEY(user_id)
                                          )""")

song_table_create = ("""CREATE TABLE songs ( 
                                         song_id  text NOT NULL,
                                         title  text NOT NULL,
                                         artist_id text NOT NULL,
                                         year  integer NOT NULL,
                                         duration  float,
                                         PRIMARY KEY(song_id)
                                         )""")

artist_table_create = ("""CREATE TABLE artists ( 
                                        artist_id text NOT NULL, 
                                        name  text NOT NULL,
                                        location  text,
                                        latitude   float,
                                        longitude  float,
                                        PRIMARY KEY(artist_id)
                                            )""")

time_table_create = ("""CREATE TABLE time ( 
                                        start_time timestamp  NOT NULL,
                                        hour integer  NOT NULL,
                                        day integer  NOT NULL,
                                        week integer  NOT NULL,
                                        month integer  NOT NULL,
                                        year integer  NOT NULL, 
                                        weekday text  NOT NULL,
                                        PRIMARY KEY(start_time)
                                        )""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent) 
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""                        
                        )

user_table_insert = ("""INSERT INTO users(user_id,first_name,last_name, gender, level) 
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level
                    """)

song_table_insert = ("""INSERT INTO songs( song_id, title, artist_id, year, duration) 
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT (song_id) DO NOTHING""")

artist_table_insert = ("""INSERT INTO artists( artist_id, name, location, latitude, longitude) 
                          VALUES(%s,%s,%s,%s,%s)
                           ON CONFLICT (artist_id) DO NOTHING""")


time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday) 
                        VALUES(%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (start_time) DO NOTHING""")

# FIND SONGS
 

song_select = ("""SELECT songs.song_id,
                         songs.artist_id 
                        FROM songs songs 
                        inner join 
                        artists artists 
                           on artists.artist_id = songs.artist_id
                        where  songs.title= %s 
                          and artists.name = %s 
                          and songs.duration =%s
                """)

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]