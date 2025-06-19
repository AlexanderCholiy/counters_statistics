import os
import zipfile
import datetime as dt

from dateutil.relativedelta import relativedelta
from core.utils import CountersStatisticDB
from core.config import Config
from core.logger import FileRotatingLogger
from core.timer import execution_time
from core.progress_bar import progress_bar
from core.save_df_2_excel import save_df_2_excel
from core.argparser import parse_args


@execution_time
def split_statistics_by_month(db_path: str):
    """
    Загружает показания счётчиков из тяжелой БД,
    разбивает их по месяцам и сохраняет в отдельные месячные базы данных.

    Логика работы:
    - Определяет граничеые временные интервалы.
    - Загружает данные порциями (пагинация) по N записей.
    - Группирует и добавляет статистику в соответствующие месячные БД.
    - Отображает прогресс выполнения.
    """
    db = CountersStatisticDB(db_path)
    start, end = db.border_timestamp
    total = db.count_records(start, dt.datetime.now())
    step = 100_000

    for index in range(0, total, step):
        progress_bar(index-1, total, 'Добавление статистики по месяцам: ')
        page_number = (index // step) + 1
        statistics = db.get_statistics_by_period(
            start=start,
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
    db = CountersStatisticDB(db_path)
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


@execution_time
def dump_and_remove_old_dbs():
    """
    Архивирует базы данных из папки Config.DATA_DIR, имена которых имеют формат
    Config.PREFIX_YYYY_MM.db и дата которых старше Config.MONTH_AGO
    месяцев, затем удаляет исходные .db файлы.
    """
    now = dt.datetime.now()

    for filename in os.listdir(Config.DATA_DIR):
        if (
            not filename.startswith(f'{Config.DB_PREFIX}_')
            or not filename.endswith('.db')
        ):
            continue

        parts = filename[:-3].split('_')
        if len(parts) < 4:
            continue

        try:
            year = int(parts[2])
            month = int(parts[3])
        except ValueError:
            continue

        months_diff = (now.year - year) * 12 + (now.month - month)
        if months_diff > Config.MONTH_AGO:
            db_path = os.path.join(Config.DATA_DIR, filename)
            zip_path = os.path.join(
                Config.DATA_DIR, filename.replace('.db', '.zip'))

            with zipfile.ZipFile(
                zip_path, 'w', compression=zipfile.ZIP_DEFLATED
            ) as zipf:
                zipf.write(db_path, arcname=filename)

            os.remove(db_path)
            print(
                f'База {filename} архивирована в {zip_path} '
                'и исходный файл удалён.'
            )


if __name__ == '__main__':
    args = parse_args()
    logger = FileRotatingLogger(
        Config.LOG_DIR, debug=Config.DEBUG).get_logger()

    if args.split_statistics_by_month:
        db_path = r'data\counters_statistics_2025_01.db'
        split_statistics_by_month(db_path)
    elif args.save_counter_statistic:
        db_path = r'data\counters_statistics_2025_01.db'
        start = dt.datetime.now() - relativedelta(months=Config.MONTH_AGO)
        end = dt.datetime.now()
        modem_ip = '10.24.7.132'
        save_counter_statistic(db_path, start, end, modem_ip)
    elif args.dump_and_remove_old_dbs:
        try:
            dump_and_remove_old_dbs()
        except Exception:
            logger.exception('Ошибка при архивации БД')
    else:
        print('Не указана команда. Используйте --help для справки.')
