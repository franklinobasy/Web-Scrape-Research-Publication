#!/usr/bin/env python3
'''
Module for ETL Pipeline

author: Franklin Obasi <franklinobasy@gmail.com>
'''

from publication.extract import Extract
from publication.transform import convert_to_dataframe
from publication.load import create_connection, dataframe_to_database, shutdown_connection

class Pipeline():
    '''ETL pipeline'''

    def __init__(
            self,
            pipeline_name: str,
            export_path: str,
            connection_param: dict,
            table_name: str
        ) -> None:
        
        self.pipeline_name = pipeline_name
        self.export_path = export_path
        self.connection_param = connection_param
        self.table_name = table_name
        self.df = None
    
    def extract(self):
        '''extraction method'''
        print(">>>> STEP 1: Extraction started >>>>>")
        print("\textraction in progress...")

        extraction = Extract()
        extraction.run()
        extraction.result_tojson(self.export_path)

        print("[SUCCESS]: Extraction completed")

    def transform(self):
        '''transform method
        '''
        print(">>>> STEP 2: Transformation started >>>>>")
        print("\ttransformation in progress...")
        
        self.df = convert_to_dataframe(self.export_path)

        print("[SUCCESS]: tranformation completed")

    def load(self):
        '''load method'''
        print(">>>> STEP 3: Load started >>>>>")
        print("\tload in progress...")

        print("\tcreating connection...")
        connection = create_connection(**self.connection_param)
        print("\tconnection established!")

        print("\tloading to database...")
        if dataframe_to_database(connection, self.df, self.table_name):
            print("[SUCCESS]: Load to database successful")
        else:
            print("[ERROR]: load to database failed")
            raise Exception("[ERROR]: load to database failed")

        print("\tshutting down database connection")
        if shutdown_connection(connection):
            print("[SUCCESS]successfully shutdown connection")
        else:
            print("[WARNING]: connection shutdown failed")

        print("[SUCCESS]: load to database completed")


    def execute(self):
        '''
        Run pipeline
        '''
        self.extract()
        self.transform()
        self.load()

