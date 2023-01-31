import logging
import sqlite3
from typing import Union

from abc import ABC, abstractmethod
from contextlib import contextmanager

from exceptions import DBConnectionError


class BaseDBConnector(ABC):

    @abstractmethod
    def connection(self):
        pass

    @abstractmethod
    def execute_sql(self, row_query):
        pass

    @staticmethod
    def close_connection(connection):
        if connection:
            logging.info('Closing database connection..')
            connection.close()
        else:
            logging.debug('Database connection already close')


class SQLiteConnector(BaseDBConnector):

    def __init__(self, db_name: str):
        self.db_name = db_name

    @contextmanager
    def connection(self, cursor=True):
        connection = None
        try:
            connection = sqlite3.connect(self.db_name)
            yield connection
        except sqlite3.Error as err:
            logging.error(f'Connect to database failed with error: {str(err)}')
            raise DBConnectionError(f'Failed connection to database: {str(err)}')
        finally:
            self.close_connection(connection)

    def execute_sql(self, row_query, commit=False) -> Union[list, None]:
        with self.connection() as db:
            cursor = db.cursor()
            cursor.execute(row_query)
            if commit:
                db.commit()
            else:
                return cursor.fetchall()

    def execute_many(self, row_query: str, params: list) -> None:
        with self.connection() as db:
            cursor = db.cursor()
            cursor.executemany(row_query, params)
            db.commit()
