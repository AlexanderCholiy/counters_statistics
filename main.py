import os
import datetime as dt
from dateutil.relativedelta import relativedelta

from core.utils import CountersStatisticDB
from core.config import Config
from core.timer import execution_time
from core.progress_bar import progress_bar
from core.save_df_2_excel import save_df_2_excel


@execution_time
def split_statistics_by_month():
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


def save_counter_statistic(db_path, start, end, modem_ip: str):
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
    # split_statistics_by_month()
    month_ago = dt.datetime.now() - relativedelta(months=Config.MONTH_AGO)
    db_path = (
        r'C:\Users\a.choliy\Desktop\test\data\counters_statistics_2025_04.db'
    )
    save_counter_statistic(
        db_path,
        month_ago,
        dt.datetime.now(),
        modem_ip='10.24.7.132',
    )
