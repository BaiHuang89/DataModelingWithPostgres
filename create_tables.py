# create_tables.py
# Copyright (C) 2019 Yanru Wang <michelle.yanru.wang@gmail.com> 
#
# This module is a part of online course project and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""

Methods to create database and tables before etl process.

"""

import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """Create database sparkifydb.
    """
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """Drop all tables before create.

    Parameters
    ----------
    cur: psycopg2.cursor
    conn: psycopg2.connection

    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create all tables.

    Parameters
    ----------
    cur: psycopg2.cursor
    conn: psycopg2.connection

    """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()