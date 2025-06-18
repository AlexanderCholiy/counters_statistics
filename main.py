import os
import datetime as dt
from dateutil.relativedelta import relativedelta

from core.utils import CountersStatisticDB
from core.config import Config
from core.timer import execution_time
from core.progress_bar import progress_bar
from core.save_df_2_excel import save_df_2_excel
from core.argparser import parse_args


@execution_time
def split_statistics_by_month():
    """
    Загружает показания счётчиков за последние Config.MONTH_AGO месяцев,
    разбивает их по месяцам и сохраняет в отдельные месячные базы данных.

    Логика работы:
    - Определяет временной интервал начиная с текущей даты минус
    Config.MONTH_AGO месяцев.
    - Загружает данные порциями (пагинация) по 100000 записей.
    - Группирует и добавляет статистику в соответствующие месячные БД.
    - Отображает прогресс выполнения.
    """
    ...
    db = CountersStatisticDB()
    _, end = db.border_timestamp
    month_ago = dt.datetime.now() - relativedelta(months=Config.MONTH_AGO)
    total = db.count_records(month_ago, dt.datetime.now())
    step = 100_000

    for index in range(0, total, step):
        progress_bar(index, total, 'Добавление статистики по месяцам: ')
        page_number = (index // step) + 1
        statistics = db.get_statistics_by_period(
            start=month_ago,
            end=end,
            page_number=page_number,
            page_size=step
        )
        db.add_statistics_to_monthly_db(statistics)

    progress_bar((total-1), total, 'Добавление статистики по месяцам: ')


@execution_time
def save_counter_statistic(
    db_path: str, start: dt.datetime, end: dt.datetime, modem_ip: str
):
    """
    Сохраняет статистику конкретного счетчика по IP за заданный период в
    Excel-файл.

    Аргументы:
        db_path (str): Путь к базе данных со статистикой.
        start (datetime): Начальная дата и время выборки.
        end (datetime): Конечная дата и время выборки.
        modem_ip (str): IP-адрес модема, для которого сохраняются данные.

    Логика работы:
    - Подключается к базе данных по указанному пути.
    - Удаляет существующий Excel-файл статистики, если он есть.
    - Загружает данные порциями по 10000 записей с пагинацией.
    - Преобразует данные в DataFrame и подготавливает к сохранению.
    - Сохраняет каждый набор данных на отдельный лист Excel-файла с именем
    листа, включающим IP и номер страницы.
    - Выводит сообщение о результате сохранения.
    """
    db = CountersStatisticDB()
    db.switch_database(db_path)
    step = 10_000
    page_number = 1

    if os.path.isfile(Config.STATISTIC_PATH):
        os.remove(Config.STATISTIC_PATH)

    while True:
        statistics = db.get_statistics_by_period(
            start=start,
            end=end,
            page_number=page_number,
            page_size=step,
            modem_ip=modem_ip
        )
        if not statistics:
            break

        df = db.prepare_statistics(db.statistics_to_dataframe(statistics))
        sheet_name = f'{modem_ip} ({page_number})'
        save_df_2_excel(df, Config.STATISTIC_PATH, sheet_name)

        page_number += 1

    if statistics or page_number > 1:
        print(
            f'Показания счетчика с ip: {modem_ip} '
            f'сохранены: {Config.STATISTIC_PATH}'
        )
    else:
        print('Показания отсутствуют')


if __name__ == '__main__':
    args = parse_args()

    if args.split_statistics_by_month:
        split_statistics_by_month()
    elif args.save_counter_statistic:
        db_path = 'counters_statistics.db'
        start = dt.datetime.now() - relativedelta(months=Config.MONTH_AGO)
        end = dt.datetime.now()
        modem_ip = '10.24.7.132'
        save_counter_statistic(db_path, start, end, modem_ip)
    else:
        print('Не указана команда. Используйте --help для справки.')
