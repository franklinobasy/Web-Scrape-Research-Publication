#!/usr/bin/env python3
'''
Module for Loading to postgres database

author: Franklin Obasi <franklinobasy@gmail.com>
'''

from typing import List, Union
import pandas as pd
import psycopg2 as pg
from sqlalchemy import create_engine

def create_connection(user: str, password: str, database: str, host: str, port: str):
    '''
    Create a connection to the postgres database

    Arguments:
        user: postgres username
        password: postgres user's password
        database: name of the database
        host: hostname or IP
        port: postgress application port

    Return:
        connection
    '''
    try:
        connection = create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/{database}"
        )

    except:
        raise ValueError('Something went wrong while trying to connect to postgress.Make sure your credentials are valid'
                         )
    return connection

def dataframe_to_database(connection, df: pd.DataFrame, table_name: str = 'publication') -> bool:
    '''
    Loads dataframe as table to the database

    Argument:
        connection: database connection
        df: dataframe to store
        table_name: name of the table, default is 'publication'

    Return:
        True if successful else False
    '''

    try:
        df.to_sql(table_name, connection, if_exists='replace')
    except:
        return False
    return True

def shutdown_connection(connection):
    '''
    Close the connection

    Argument:
        connection: database connection

    Return:
        True if connection closes successfully else False
    '''
    try:
        if isinstance[connection, list]:
            for conn in connection:
                conn.close()
        else:
            connection.close()
    except:
        return False
    
    return True

if __name__ == "__main__":
    from transform import covert_to_dataframe

    df = covert_to_dataframe('extraction.json')

    connection = create_connection('', '', '', '', '')

    if dataframe_to_database(connection, df):
        print("Succesfully loaded to postgres database")

    else:
        print("Loading to database failed")

    shutdown_connection(connection)