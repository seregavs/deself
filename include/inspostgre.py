import psycopg2, psycopg2.extras
import os
import logging
import platform
from airflow.providers.postgres.hooks.postgres import PostgresHook


class InsertCSVPostre_Base:
    """ 
    Base class for inserting data to postgre tables from csv
    """
    def __init__(self, c_fullpath: str):
        """ 
         c_fullpath - directory with source csv-files
        """
        self.c_fullpath = c_fullpath