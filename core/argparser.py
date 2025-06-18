import argparse


def parse_args():
    parser = argparse.ArgumentParser(
        description='Управление скриптом статистики счетчиков'
    )
    parser.add_argument(
        '--split_statistics_by_month',
        action='store_true',
        help='Запустить функцию split_statistics_by_month'
    )
    parser.add_argument(
        '--save_counter_statistic',
        action='store_true',
        help='Запустить функцию save_counter_statistic'
    )
    parser.add_argument(
        '--dump_and_remove_old_dbs',
        action='store_true',
        help='Запустить функцию dump_and_remove_old_dbs'
    )
    return parser.parse_args()
