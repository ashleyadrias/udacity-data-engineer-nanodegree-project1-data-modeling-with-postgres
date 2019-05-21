import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import datetime as dt

"""
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """


def process_song_file(cur, filepath):
     """
        Description: This function can be used to read all files in the filepath (data/song_data).
        Convert 'ts' columns values to timestamp. Create the song and artist tables.

        Arguments:
            filepath: song data file path. 

        Returns:
            None
    """
        
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
        Description: This function can be used to read all files in the filepath (data/log_data),
        convert 'ts' columns values to timestamp. Create the songsplay table.

        Arguments:
            filepath: log data file path. 

        Returns:
            None
    """
        
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = df.copy()
    t['ts'] = pd.to_datetime(t['ts'])
    
    # insert time data records
    time_data = t['ts'].tolist()
    #column_labels = ['timestamp','hour','day','week','month','year','weekday']
    #time_df = pd.DataFrame({column_labels:time_data})
    
    timestamp = []
    time_data_hour = []
    time_data_day = []
    time_data_week = []
    time_data_month = []
    time_data_year = []
    time_data_weekday = []

    for timestamp in time_data:
        timestamp = timestamp
        time_data_hour.append(timestamp.hour)
        time_data_day.append(timestamp.day)
        time_data_week.append(timestamp.week)
        time_data_month.append(timestamp.month)
        time_data_year.append(timestamp.year)
        time_data_weekday.append(timestamp.weekday())
        
    time_df = pd.DataFrame({
    'start_time': timestamp,
    'hour':time_data_hour,
    'day':time_data_day,
    'week':time_data_week,
    'month':time_data_month,
    'year':time_data_year,
    'weekday':time_data_weekday})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

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
        time_convert = dt.datetime.fromtimestamp(row.ts / 1e3)
        songplay_data = (time_convert, row.userId, row.level, songid, artistid,
                         row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
        Description: This function can be used to read all files in the filepath (data/song_data)

        Arguments:
            filepath: log data file path. 

        Returns:
            None
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()