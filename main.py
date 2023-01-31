"""App entrypoint"""
import logging

from handlers import ReportHandler


def run(db_name: str, table_name: str, xlsx_path: str) -> None:
    handler = ReportHandler(db_name, table_name, xlsx_path)

    logging.debug('Trying creation database and table')
    handler.create_db_table()

    logging.debug('Inserting data..')
    handler.inserting_data()

    for item in handler.get_report():
        logging.info('Date: {} :: Qliq: {} :: Qoil: {}'.format(*item))
        print('Date: {} :: Qliq: {} :: Qoil: {}'.format(*item))


if __name__ == '__main__':
    run('example.db', 'simple_report', 'simple_data.xlsx')
