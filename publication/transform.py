#!/usr/bin/env python3
'''
Module for Transformation Phase

author: Franklin Obasi
'''

import json
import pandas as pd
from typing import List


def load_json(path: str) -> List[dict]:
    '''
    Loads data from json file

    Argument:
        path: path to json file

    Return:
        list object
    '''

    if path.split('.')[-1] != 'json':
        raise ValueError('Expects a json file')

    try:
        with open(path, 'r') as f:
            data = json.load(f)

    except FileNotFoundError:
        raise ValueError('Could not find file')

    return data


def convert_to_dataframe(path: str, columns: List[str] = []) -> pd.DataFrame:
    '''
    Convert data to dataframe

    Arguments:
        data: list of dictionary object containing data
        columns: columns to return

    Return:
        Pandas dataframe
    '''

    data = load_json(path)

    df = pd.DataFrame(data)

    if columns:
        df = df[columns]

    return df


if __name__ == "__main__":
    # load and  transform
    df = convert_to_dataframe('extraction.json')

    # display
    print(df.head())
