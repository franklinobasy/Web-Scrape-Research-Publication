#!/usr/bin/env python3
'''
Module for ETL Pipeline

author: Franklin Obasi <franklinobasy@gmail.com>
'''

import logging

from publication.extract import Extract
from publication.transform import convert_to_dataframe
from publication.load import create_connection, dataframe_to_database, shutdown_connection

import sys

logging.basicConfig(format='%(process)d-%(levelname)s - %(asctime)s - %(message)s',
                    stream=sys.stdout, level=logging.DEBUG
                    )

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
        logging.info(">>>> STEP 1: Extraction started >>>>>")
        logging.info("extraction in progress...")

        extraction = Extract()
        extraction.run()
        extraction.result_tojson(self.export_path)

        logging.info("[SUCCESS]: Extraction completed")

    def transform(self):
        '''transform method
        '''
        logging.info(">>>> STEP 2: Transformation started >>>>>")
        logging.info("\ttransformation in progress...")

        self.df = convert_to_dataframe(self.export_path)

        logging.info("[SUCCESS]: tranformation completed")

    def load(self):
        '''load method'''
        logging.info(">>>> STEP 3: Load started >>>>>")
        logging.info("load in progress...")

        logging.info("creating connection...")
        connection = create_connection(**self.connection_param)
        logging.info("connection established!")

        logging.info("loading to database...")
        if dataframe_to_database(connection, self.df, self.table_name):
            logging.info("[SUCCESS]: Load to database successful")
        else:
            logging.error("[ERROR]: load to database failed")
            raise Exception("[ERROR]: load to database failed")

        logging.info("shutting down database connection")
        if shutdown_connection(connection):
            logging.info("[SUCCESS]successfully shutdown connection")
        else:
            logging.warning("[WARNING]: connection shutdown failed")

        logging.info("[SUCCESS]: load to database completed")

    def execute(self):
        '''
        Run pipeline
        '''
        self.extract()
        self.transform()
        self.load()
