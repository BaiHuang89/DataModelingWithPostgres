# test.py
# Copyright (C) 2019 Yanru Wang <michelle.yanru.wang@gmail.com> 
#
# This module is a part of online course project and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""

Methods to check the result of ETL process and analysis user activities.

"""

import pandas as pd
from sqlalchemy import create_engine
from sql_queries import analysis_most_popular_artist, analysis_most_popular_song

def select(engine, table_name):
    """Select top 5 records from table and output to stdout.

    Parameters
    ----------
    engine: sqlalchemy.base.Engine
            Instance of sqlalchemy.base.Engine.
    table_name: string
            The name of table to select.

    """

    df = pd.read_sql_table(table_name = table_name, con = engine)
    # print out top 5 rows
    print('\n\n--------------------{}--------------------'.format(table_name))
    print(df.head(5))

def check(engine):
    """Check data of all tables: songs, artists, time, users, songplays

    Parameters
    ----------
    engine: sqlalchemy.base.Engine
            Instance of sqlalchemy.base.Engine.

    """

    # query data from tables
    select(engine, 'songs')
    select(engine, 'artists')
    select(engine, 'time')
    select(engine, 'users')
    select(engine, 'songplays')

def analysis(engine):
    """Query the most popular song and artist in 2018.

    Parameters
    ----------
    engine: sqlalchemy.base.Engine
            Instance of sqlalchemy.base.Engine.

    """

    print('\n\n-------------The Most popular song in 2018---------------')
    df = pd.read_sql_query(sql = analysis_most_popular_song, con = engine, params = ['2018'])
    print(df)

    print('\n\n-------------The Most popular artist in 2018---------------')
    df = pd.read_sql_query(sql = analysis_most_popular_artist, con = engine, params = ['2018'])
    print(df)

def main():
    # connect to sparkifydb database
    engine_string = 'postgresql://student:student@127.0.0.1:5432/sparkifydb'
    engine = create_engine(engine_string)

    # check the result of ETL process
    check(engine)

    # do some analysis
    analysis(engine)

    # close all connections
    engine.dispose()


if __name__ == '__main__':
    main()