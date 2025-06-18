import os


class Config:
    ROOT_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
    DATA_DIR = os.path.join(ROOT_DIR, 'data')
    DB_PATH = os.path.join(ROOT_DIR, 'counters_statistics.db')
    STATISTIC_PATH = os.path.join(ROOT_DIR, 'data', 'counter_statistic.xlsx')

    MONTH_AGO = 3
