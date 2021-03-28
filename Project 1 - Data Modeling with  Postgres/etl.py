import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Reads the JSON song file and loads the data in songs and artists table.
    """ 
    # open song file
    df =pd.read_json(filepath, lines=True)  
    artist_id, artist_latitude, artist_location, artist_longitude, artist_name, duration, num_songs, song_id, title, year = df.values[0]
    # insert song record
    song_data =  list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data =  list(df[['artist_id' ,'artist_name', 'artist_location','artist_latitude','artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Reads the JSON log file and filters the data by NextSong 
    - Extract user information and load the user table
    - Extract datetime information from one of the fields in the JSON log file called ts and load the time table
    - Get the songid and artistid by querying the songs and artists tables to find matches based on the following data in the log file -: song title, artist name, and song duration time 
    - Load the songplays table using the information from log file along with the corresonding matching songid and artistid. 
    """     
    # open log file
    df = pd.read_json(filepath, lines=True)  

    # filter by NextSong action
    df=df.loc[df.page=='NextSong']
    
    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = df['ts']  
 
    # insert time data records
    time_data = [df['ts'],t.dt.hour.values, t.dt.day.values, t.dt.weekofyear.values, t.dt.month.values, t.dt.year.values, t.dt.weekday.values]
    column_labels = ['starttime','hour','day','weekofyear','month', 'year', 'weekday'] 
    time_df = pd.DataFrame(dict(list(zip(column_labels,time_data))))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row.ts,row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]        
        cur.execute(songplay_table_insert, songplay_data)  


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def main(): 
    """
 - Connects to the sparkify database and connects cursor.
 - Loads the data in the files from the 2 folders data/song_data and data/log_data into the tables in the sparkify database
 - Closes connection.
    """ 
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()