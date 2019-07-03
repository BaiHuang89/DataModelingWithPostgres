import io
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from collections import OrderedDict


def bulk_insert_data(cur, df, table, columns):
    # new file-like object
    data_buffer = io.StringIO()
    
    # copy data of pandas.DataFrame to file-like object
    # header and index should be ignored 
    df.to_csv(data_buffer, header=False, index=False, sep='\t')

    # !!! Be sure to reset the position to the start of the stream
    # !!! Spent a lot of time to figure out why no records were written and no errors accured.
    data_buffer.seek(0)

    # use the copy_from API of Psycopg2.cursor
    # specific columns for Non-Serial column, otherwise copy_from would try to insert data to the Serial column.
    cur.copy_from(file=data_buffer, table=table, sep='\t', null='', columns=columns)

    # print('{} records written into table {}'.format(cur.rowcount, table))

    # close the buffer
    data_buffer.close()

def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = t.map(lambda x: [x, x.hour, x.day, x.week, x.month, x.year, x.weekday()]).tolist()
    column_labels = (['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'])
    time_df = pd.DataFrame.from_dict([OrderedDict(zip(column_labels, data)) for data in time_data])

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
           songid, artistid = results[0], results[1] 
        else:  
            songid, artistid = None, None

        # insert songplay record
        # # abandon the log if no related song and artist were found.
        if songid is not None and artistid is not None:
            songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)

def process_log_file_bulk(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # process time data
    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    # map time serie to dataframe
    time_data = t.map(lambda x: [x, x.hour, x.day, x.week, x.month, x.year, x.weekday()]).tolist()
    column_labels = (['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'])
    time_df = pd.DataFrame.from_dict([OrderedDict(zip(column_labels, data)) for data in time_data])
    # bulk insert time records into TEMP table to avoid violating unique constraint on start_time
    cur.execute(time_temp_table_create)
    bulk_insert_data(cur, time_df, 'time_temp', None)

    # process users data
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    # convert data type of user_id to integer to be consistent with table's definition
    user_df = user_df.astype({'userId': int})
    # user_df['user_id'] = pd.to_numeric(user_df['user_id'], downcast='integer')
    # bulk insert user records into TEMP table to avoid violating unique constraint on user_id
    cur.execute(user_temp_table_create)
    bulk_insert_data(cur, user_df, 'users_temp', None)

    # process songplays data
    songplay_df = df[['ts', 'userId', 'level', 'sessionId', 'location', 'userAgent']]
    # add songId and artistiId columns with default value None
    songplay_df.insert(3, 'songId', None)
    songplay_df.insert(4, 'artistId', None)
    # populate songId and artistId
    for index, row in df.iterrows(): 
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results[0], results[1] 
        else:
            songid, artistid = None, None

        if songid is not None and artistid is not None:
            songplay_df.at[index, 'songId'] = songid
            songplay_df.at[index, 'artistId'] = artistid

    # filter out records where songid or artistid is NULL
    songplay_df = songplay_df[songplay_df['songId'].notnull()]
    songplay_df = songplay_df[songplay_df['artistId'].notnull()]
    # convert ts to datetime
    songplay_df['ts'] = songplay_df['ts'].map(lambda x: pd.to_datetime(x, unit='ms'))
    # bulk insert songplays records
    column_labels = (['start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent'])
    bulk_insert_data(cur, songplay_df, 'songplays', column_labels)


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


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    # process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file_bulk)

    # copy data from temp table to real table
    # Using COPY to insert in bulk can not simply skip unique constraint.
    for query in migrate_from_temp_table:
        cur.execute(query)
        conn.commit()

    # get the count of records for songplays.
    cur.execute('select * from songplays;')
    print('Total {} records written in songplays.'.format(cur.rowcount))

    conn.close()


if __name__ == "__main__":
    main()