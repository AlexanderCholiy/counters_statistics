import os
import datetime as dt

from pandas import DataFrame
from dateutil.relativedelta import relativedelta
from core.utils import CountersStatisticDB
from core.config import Config
from core.logger import FileRotatingLogger
from sqlalchemy.exc import OperationalError
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
    start: dt.datetime, end: dt.datetime, modem_ip: str
):
    """
    Сохраняет статистику конкретного счетчика по IP за заданный период в
    Excel-файл из всех подходящих баз данных.

    Аргументы:
        start (datetime): Начальная дата и время выборки.
        end (datetime): Конечная дата и время выборки.
        modem_ip (str): IP-адрес модема, для которого сохраняются данные.

    Логика работы:
    - Удаляет существующий Excel-файл статистики, если он есть.
    - Подключается к базам данных которые соотв. фильтру по дате.
    - Загружает данные порциями по N записей с пагинацией.
    - Преобразует данные в DataFrame и подготавливает к сохранению.
    - Сохраняет каждый набор данных на отдельный лист Excel-файла с именем
    листа, включающим IP и номер страницы.
    - Выводит сообщение о результате сохранения.
    """
    databases = []

    for filename in os.listdir(Config.DATA_DIR):
        if (
            not filename.startswith(f'{Config.DB_PREFIX}_')
            or not filename.endswith('.db')
        ):
            continue

        parts = filename.replace('.db', '').split('_')
        if len(parts) < 4:
            continue

        try:
            year = int(parts[2])
            month = int(parts[3])
            file_date = dt.datetime(year, month, 1)
        except ValueError:
            continue

        if start <= file_date <= end:
            full_path = os.path.join(Config.DATA_DIR, filename)
            databases.append(full_path)

    if os.path.isfile(Config.STATISTIC_PATH):
        os.remove(Config.STATISTIC_PATH)

    if not databases:
        print('Нет подходящих незаархивированных БД для выбранного периода.')
        return

    step = 10_000
    page_number = 1
    modem_dates: dict[dt.date, int] = {}

    for index, db_file in enumerate(sorted(databases)):
        progress_bar(index, len(databases), 'Поиск данных: ')
        db = CountersStatisticDB(db_file)
        filename = os.path.basename(db_file)
        parts = filename.replace('.db', '').split('_')
        year = int(parts[2])
        month = int(parts[3])
        sheet_prefix = f'{year}_{month:02d}'

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
            counts = df['timestamp'].dt.date.value_counts()
            for date, count in counts.items():
                modem_dates[date] = modem_dates.get(date, 0) + count
            sheet_name = f'{sheet_prefix} ({page_number})'
            save_df_2_excel(df, Config.STATISTIC_PATH, sheet_name)

            page_number += 1

    if statistics or page_number > 1:
        print(
            f'Показания счетчика с ip: {modem_ip} '
            f'сохранены: {Config.STATISTIC_PATH}'
        )
        df_dates = DataFrame(
            sorted(modem_dates.items()),
            columns=['Дата', 'Количество записей']
        )
        print(df_dates.to_string(index=False))
    else:
        print('В указанный период не найдено ни одной записи.')
        print(f'Диапазон дат: {start.date()} — {end.date()}')


@execution_time
def zip_and_remove_old_dbs():
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
            CountersStatisticDB.zip_db(db_path, Config.DATA_DIR)


@execution_time
def statistics_2_db():
    """
    Загружает данные счётчиков из .csv или .gz файлов в основную базу данных,
    распределяя записи по отдельным месячным БД.

    Логика работы:
    - Создаёт/подключается к основной БД текущего месяца.
    - Ищет не обработанные файлы статистики (файлы, не вошедшие в БД).
    - Читает данные из каждого файла в виде DataFrame.
    - Разбивает DataFrame на порции по 100 000 записей.
    - Каждую порцию преобразует в объекты модели Statistic.
    - Добавляет записи в соответствующие месячные БД, исключая дубликаты.
    """
    CountersStatisticDB().statistics_2_db()


@execution_time
def remove_processed_csv_gz():
    """
    Удаляет все .csv.gz файлы из директории со статистикой
    (Config.STATISTIC_DIR).

    Логика работы:
    - Перебирает все файлы в директории.
    - Отбирает только те, что заканчиваются на .csv.gz и содержат валидную
    дату в имени.
    - Удаляет такие файлы из файловой системы.
    """
    for filename in os.listdir(Config.STATISTIC_DIR):
        if not filename.endswith('.csv.gz'):
            continue

        try:
            dt.datetime.strptime(filename[:10], '%Y-%m-%d').date()
        except ValueError:
            continue

        os.remove(os.path.join(Config.STATISTIC_DIR, filename))


if __name__ == '__main__':
    args = parse_args()
    logger = FileRotatingLogger(
        Config.LOG_DIR, debug=Config.DEBUG).get_logger()

    if args.split_statistics_by_month:
        db_path = r'data/counters_statistics_2025_01.db'
        split_statistics_by_month(db_path)
    elif args.save_counter_statistic:
        if not args.modem_ip:
            raise ValueError(
                'Ошибка: для --save_counter_statistic '
                'необходимо указать --modem_ip'
            )
        start = dt.datetime.now() - relativedelta(months=Config.MONTH_AGO)
        end = dt.datetime.now()
        modem_ip = args.modem_ip
        try:
            save_counter_statistic(start, end, modem_ip)
        except OperationalError as e:
            if 'database is locked' in str(e):
                print('База данных занята, пожалуйста, подождите.')
            else:
                raise
    elif args.zip_and_remove_old_dbs:
        try:
            zip_and_remove_old_dbs()
        except Exception:
            logger.exception('Ошибка при архивации БД')
            raise
        else:
            logger.info('Архивация баз данных завершена')
    elif args.statistics_2_db:
        try:
            statistics_2_db()
        except Exception:
            logger.exception('Ошибка при добавлении данных в БД')
            raise
        else:
            logger.info('Базы данных с показаниями счётчиков обновлены')
    elif args.remove_processed_csv_gz:
        try:
            remove_processed_csv_gz()
        except Exception:
            logger.exception('Ошибка при удалении лишних .csv.gz файлов')
            raise
        else:
            logger.info('Лишние файлы .csv.gz удалены')
    else:
        print('Не указана команда. Используйте --help для справки.')
