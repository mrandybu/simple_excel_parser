from abc import ABC
from datetime import datetime

from db_connector import SQLiteConnector
from xlsx_parser import SimpleReportParser
from utils import data_generator


class BaseHandler(ABC):
    pass


class ReportHandler(BaseHandler):

    def __init__(self, db_name, table_name, xlsx_path):
        self.db = SQLiteConnector(db_name)
        self.table_name = table_name
        self.xlsx_path = xlsx_path
        self._now = datetime.now()

    def create_db_table(self) -> None:
        query = f"""
                    CREATE TABLE IF NOT EXISTS {self.table_name} (
                      id INTEGER PRIMARY KEY AUTOINCREMENT, 
                      create_at TEXT,  
                      company VARCHAR(32) NOT NULL, 
                      fact_qliq_data1 INT DEFAULT 0 NOT NULL, 
                      fact_qliq_data2 INT DEFAULT 0 NOT NULL, 
                      fact_qoil_data1 INT DEFAULT 0 NOT NULL, 
                      fact_qoil_data2 INT DEFAULT 0 NOT NULL, 
                      forecast_qliq_data1 INT DEFAULT 0 NOT NULL, 
                      forecast_qliq_data2 DEFAULT 0 NOT NULL, 
                      forecast_qoil_data1 DEFAULT 0 NOT NULL, 
                      forecast_qoil_data2 DEFAULT 0 NOT NULL
                    );
                """

        self.db.execute_sql(query, commit=True)

    def _insert_data(self, batch: list) -> None:
        query = f"""
                    INSERT INTO {self.table_name}(create_at,
                                                  company,
                                                  fact_qliq_data1,
                                                  fact_qliq_data2,
                                                  fact_qoil_data1,
                                                  fact_qoil_data2,
                                                  forecast_qliq_data1,
                                                  forecast_qliq_data2,
                                                  forecast_qoil_data1,
                                                  forecast_qoil_data2)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                """

        self.db.execute_many(query, batch)

    @staticmethod
    def _split_to_chunks(lst: list, n: int) -> list:
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def _enrich_date(self, data: list) -> list:
        for item in data:
            item[0] = data_generator(self._now.year, self._now.month)
        return data

    def _get_inserted_data(self, batch_size=100) -> list:
        srp = SimpleReportParser(self.xlsx_path)
        values = srp.parse(only_raws=True, skip_from_top=2)
        return self._split_to_chunks(values, batch_size)

    def inserting_data(self) -> None:
        for batch in self._get_inserted_data():
            self._insert_data(self._enrich_date(batch))

    def get_report(self) -> list:
        query = """
                SELECT create_at,
                       sum(fact_qoil_data1)+sum(fact_qoil_data2)+sum(forecast_qoil_data1)+sum(forecast_qoil_data2) AS qoil,
                       sum(fact_qliq_data1)+sum(fact_qliq_data2)+sum(forecast_qliq_data1)+sum(forecast_qliq_data2) AS qliq
                FROM simple_report
                GROUP BY create_at
                """

        return self.db.execute_sql(query)
