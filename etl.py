import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Process song_data
    - Extract Data for Songs Table
    - Insert Record into Song Table
    - Extract Data for Artists Table
    - Insert Record into Artist Table
      
    Parameters:
    cur:      POSTGRES cursor object
    filepath: a str holding path to a json file
    
    Returns:
    None.  Inserts data into songs table and artist table
    """
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert song record
    song_data = list(df.values)
    song_data = (song_data[6], song_data[7], song_data[1], song_data[9], song_data[8])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df.values)
    artist_data = (artist_data[1], artist_data[5], artist_data[4], artist_data[2], artist_data[3])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Perform ETL on log files and load a records into each table
    - Extract Data for Time Table
    - Insert Records into Time Table
    - Extract Data for Users Table
    - Insert Records into Users Table
    - Extract Data and Songplays Table
    - Insert Records into Songplays Table
    
    Parameters:
    cur:      POSTGRES cursor object
    filepath: a string holding the filepath to the JSON files
    
    Returns:
    NONE.  Inserts data into users, songs and songplays table
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [t,t.dt.hour,t.dt.day,t.dt.week, t.dt.month, t.dt.year,t.dt.weekday_name]
    column_labels = ['start_time','hour','day','week','month','year','weekday']
    time_df = pd.DataFrame(dict(zip(column_labels,time_data)))

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
        songplay_data = (pd.to_datetime(row.ts,unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Get all files matching extension from directory
    - Get total number of files found
    - Iterate over files and process
    
    Parameters:
    cur:      POSTGRES cursor object      
    conn:     POSTGRES connection object
    filepath: a string holding a filepath
    func:     a process function defined within the code
    
    Returns:
    None.  But calls the process functions to insert data into tables
    """
    
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


def main():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()