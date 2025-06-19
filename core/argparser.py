import argparse


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            'Инструмент для управления обработкой '
            'и сохранением статистики счётчиков. '
            'Подробное описание функций см. соответствующие докстринги.'
        )
    )
    parser.add_argument(
        '--split_statistics_by_month',
        action='store_true',
        help='Разделить статистику БД по месяцам (split_statistics_by_month).'
    )
    parser.add_argument(
        '--save_counter_statistic',
        action='store_true',
        help=(
            'Сохранить статистику счётчиков по IP-адресу за указанный период '
            '(save_counter_statistic).'
        )
    )
    parser.add_argument(
        '--zip_and_remove_old_dbs',
        action='store_true',
        help=(
            'Архивировать и удалить устаревшие базы данных '
            '(zip_and_remove_old_dbs).'
        )
    )
    parser.add_argument(
        '--statistics_2_db',
        action='store_true',
        help=(
            'Сохранить статистику счётчиков по IP-адресу за указанный период '
            '(save_counter_statistic). Требует --modem_ip.'
        )
    )
    parser.add_argument(
        '--modem_ip',
        type=str,
        help=(
            'IP-адрес модема для сохранения статистики '
            '(обязателен с --save_counter_statistic).'
        )
    )
    parser.add_argument(
        '--remove_processed_csv_gz',
        action='store_true',
        help=('Удаление лишних .csv.gz файлов (remove_processed_csv_gz)')
    )
    return parser.parse_args()
