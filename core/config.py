import os


class Config:
    ROOT_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
    DATA_DIR = os.path.join(ROOT_DIR, 'data')
    LOG_DIR = os.path.join(ROOT_DIR, 'log')
    STATISTIC_DIR = '/var/www/data/counters_history'

    DB_PREFIX = 'counters_statistics'
    STATISTIC_PATH = os.path.join(ROOT_DIR, 'data', f'{DB_PREFIX}.xlsx')
    MONTH_AGO = 1
    DEBUG = True
