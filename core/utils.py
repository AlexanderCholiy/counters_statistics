import gzip
import os
import zipfile
import datetime as dt
from collections import defaultdict
from typing import Iterator

import pandas as pd
from dateutil.relativedelta import relativedelta
from pandas.core.series import Series
from sqlalchemy import (
    create_engine as sqlalchemy_create_engine, inspect, MetaData, tuple_, func
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine

from .models import Statistic, Base
from .config import Config
from .progress_bar import progress_bar


class CountersStatisticDB(Config):

    def __init__(self, db_path: str | None = None):
        today = dt.datetime.now()
        db_name = f'{self.DB_PREFIX}_{today.year}_{today.month:02d}.db'
        db_path = db_path or os.path.join(self.DATA_DIR, db_name)
        self.engine = self.create_engine(db_path)
        Base.metadata.create_all(self.engine)
        self.metadata = MetaData()
        self.inspector = inspect(self.engine)
        self.session = sessionmaker(bind=self.engine)

    def create_engine(self, db_path: str) -> Engine:
        """Создаёт движок базы данных, распаковывая zip при необходимости."""
        # Если передан zip-файл — распаковать
        if db_path.endswith('.zip') and os.path.isfile(db_path):
            extract_dir = os.path.dirname(db_path)
            db_name = os.path.basename(db_path).replace('.zip', '.db')
            db_path = os.path.join(extract_dir, db_name)
            self.unzip_db(
                db_path.replace('.db', '.zip'), extract_dir, overwrite=True)

        # Если указан .db, но файл отсутствует, а zip с таким же именем есть
        elif db_path.endswith('.db') and not os.path.isfile(db_path):
            zip_path = db_path.replace('.db', '.zip')
            if os.path.isfile(zip_path):
                extract_dir = os.path.dirname(db_path)
                self.unzip_db(zip_path, extract_dir, overwrite=True)

        return sqlalchemy_create_engine(
            f'sqlite:///{db_path}', echo=self.DEBUG)

    def switch_database(self, db_path: str):
        """Переключение на другую базу данных"""
        self.engine = self.create_engine(db_path)
        Base.metadata.create_all(self.engine)
        self.inspector = inspect(self.engine)
        self.session = sessionmaker(bind=self.engine)

    def db_structure(self):
        """Структура базы данных"""
        self.metadata.reflect(bind=self.engine)
        tables = self.metadata.tables
        print(tables)

        for table_name in tables:
            print(f'Структура таблицы "{table_name}":')
            columns = self.inspector.get_columns(table_name)
            for column in columns:
                print(column)

    @property
    def border_timestamp(
        self
    ) -> tuple[dt.datetime | None, dt.datetime | None]:
        with self.session() as session:
            min_statistic = (
                session.query(Statistic).order_by(Statistic.timestamp).first()
            )
            max_statistic = (
                session.query(Statistic).order_by(Statistic.timestamp.desc())
                .first()
            )

        if min_statistic is None or max_statistic is None:
            return None, None
        return min_statistic.timestamp, max_statistic.timestamp

    def count_records(
        self,
        start: dt.datetime | None = None,
        end: dt.datetime | None = None,
    ) -> int:
        with self.session() as session:
            filters = []
            if start is not None:
                filters.append(Statistic.timestamp >= start)
            if end is not None:
                filters.append(Statistic.timestamp <= end)

            count = session.query(func.count()).select_from(Statistic)
            if filters:
                count = count.filter(*filters)
        return count.scalar()

    def create_monthly_db(self, year: int, month: int):
        """Создание базы данных для заданного месяца"""
        db_name = f'{self.DB_PREFIX}_{year}_{month:02d}.db'
        db_path = os.path.join(self.DATA_DIR, db_name)
        return self.create_engine(db_path)

    @staticmethod
    def str_to_bytes(s: str | bytes | None) -> bytes | None:
        if isinstance(s, bytes):
            return s
        if s is None or s == '':
            return None
        return bytes.fromhex(s)

    def add_statistics_to_monthly_db(self, statistics: list[Statistic]):
        """Добавление статистики в соответствующую базу данных по месяцам"""
        grouped = defaultdict(list)
        for stat in statistics:
            grouped[(stat.timestamp.year, stat.timestamp.month)].append(stat)

        for (year, month), stats_group in grouped.items():
            monthly_engine = self.create_monthly_db(year, month)
            Base.metadata.create_all(monthly_engine)
            Session = sessionmaker(bind=monthly_engine)
            with Session() as session:
                keys = [
                    (s.timestamp, s.modem_ip, s.mac, s.local_id)
                    for s in stats_group
                ]

                def chunked(iterable, size):
                    for i in range(0, len(iterable), size):
                        yield iterable[i:i + size]

                existing_keys = set()
                chunk_size = 1000  # Ограничение БД
                for chunk in chunked(keys, chunk_size):
                    partial_keys = set(
                        session.query(
                            Statistic.timestamp,
                            Statistic.modem_ip,
                            Statistic.mac,
                            Statistic.local_id
                        ).filter(
                            tuple_(
                                Statistic.timestamp,
                                Statistic.modem_ip,
                                Statistic.mac,
                                Statistic.local_id
                            ).in_(chunk)
                        ).all()
                    )
                    existing_keys.update(partial_keys)

                to_add = []
                for stat in stats_group:
                    key = (
                        stat.timestamp, stat.modem_ip, stat.mac, stat.local_id
                    )

                    if (
                        key in existing_keys
                        or any(field is None for field in key)
                    ):
                        continue

                    new_statistic = Statistic(
                        timestamp=stat.timestamp,
                        modem_ip=stat.modem_ip,
                        mac=stat.mac,
                        local_id=stat.local_id,
                        voltage_1=self.str_to_bytes(stat.voltage_1),
                        current_1=self.str_to_bytes(stat.current_1),
                        angle_1=self.str_to_bytes(stat.angle_1),
                        voltage_2=self.str_to_bytes(stat.voltage_2),
                        current_2=self.str_to_bytes(stat.current_2),
                        angle_2=self.str_to_bytes(stat.angle_2),
                        voltage_3=self.str_to_bytes(stat.voltage_3),
                        current_3=self.str_to_bytes(stat.current_3),
                        angle_3=self.str_to_bytes(stat.angle_3),
                    )
                    to_add.append(new_statistic)

                if to_add:
                    session.add_all(to_add)
                    session.commit()

    def get_statistics_by_period(
        self,
        start: dt.datetime = dt.datetime.now() - dt.timedelta(days=1),
        end: dt.datetime = dt.datetime.now(),
        page_number: int = 1,
        page_size: int = 100_000,
        modem_ip: None | str = None,
        mac: None | str = None
    ) -> list[Statistic]:
        """Статистика по счётчикам за выбранный период с пагинацией"""
        offset_value = (page_number - 1) * page_size
        with self.session() as session:
            query = session.query(Statistic)
            filters = []

            filters.append(Statistic.timestamp.between(start, end))

            if modem_ip is not None:
                filters.append(Statistic.modem_ip == modem_ip)

            if mac is not None:
                filters.append(Statistic.mac == mac)

            query = query.filter(*filters)

            return (
                query
                .order_by(Statistic.timestamp)
                .limit(page_size)
                .offset(offset_value)
                .all()
            )

    def statistics_to_dataframe(
        self, statistics: list[Statistic]
    ) -> pd.DataFrame:
        """Преобразование списка объектов Statistic в DataFrame"""
        return pd.DataFrame.from_records(
            (
                {
                    'timestamp': s.timestamp,
                    'modem_ip': s.modem_ip,
                    'mac': s.mac,
                    'local_id': s.local_id,
                    'voltage_1': s.voltage_1,
                    'current_1': s.current_1,
                    'angle_1': s.angle_1,
                    'voltage_2': s.voltage_2,
                    'current_2': s.current_2,
                    'angle_2': s.angle_2,
                    'voltage_3': s.voltage_3,
                    'current_3': s.current_3,
                    'angle_3': s.angle_3,
                }
                for s in statistics
            )
        )

    def prepare_statistics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Преобразует байтовые значения напряжения, тока и углов в десятичный
        формат.
        """
        columns = [
            'decimal_voltage_1_1',
            'decimal_voltage_1_2',
            'decimal_voltage_1_3',
            'decimal_current_1_1',
            'decimal_current_1_2',
            'decimal_current_1_3',
            'decimal_angle_1_1',
            'decimal_angle_1_2',
            'decimal_angle_1_3',

            'decimal_voltage_2_1',
            'decimal_voltage_2_2',
            'decimal_voltage_2_3',
            'decimal_current_2_1',
            'decimal_current_2_2',
            'decimal_current_2_3',
            'decimal_angle_2_1',
            'decimal_angle_2_2',
            'decimal_angle_2_3',

            'decimal_voltage_3_1',
            'decimal_voltage_3_2',
            'decimal_voltage_3_3',
            'decimal_current_3_1',
            'decimal_current_3_2',
            'decimal_current_3_3',
            'decimal_angle_3_1',
            'decimal_angle_3_2',
            'decimal_angle_3_3',
        ]
        df[columns] = None

        for index, row in df.iterrows():
            for i in range(1, 4):
                decimal_voltage = self._bytes_to_float(row[f'voltage_{i}'])
                df.at[
                    index, f'decimal_voltage_{i}_1'
                ], df.at[
                    index, f'decimal_voltage_{i}_2'
                ], df.at[
                    index, f'decimal_voltage_{i}_3'
                ] = decimal_voltage

                decimal_current = self._bytes_to_float(row[f'current_{i}'])
                df.at[
                    index, f'decimal_current_{i}_1'
                ], df.at[
                    index, f'decimal_current_{i}_2'
                ], df.at[
                    index, f'decimal_current_{i}_3'
                ] = decimal_current

                decimal_angle = self._bytes_to_float(row[f'angle_{i}'])
                df.at[
                    index, f'decimal_angle_{i}_1'
                ], df.at[
                    index, f'decimal_angle_{i}_2'
                ], df.at[
                    index, f'decimal_angle_{i}_3'
                ] = decimal_angle

        return df.drop_duplicates().reset_index(drop=True)

    def _bytes_to_float(
        self, byte_data: bytes
    ) -> tuple[int | None, int | None, int | None]:
        if not byte_data:
            return (None, None, None)

        if isinstance(byte_data, bytes):
            hex_str = byte_data.hex()
        else:
            hex_str = byte_data

        if not hex_str.startswith('07'):
            hex_str = '07' + hex_str

        hex_value = bytes.fromhex(hex_str)

        if len(hex_value) < 4:
            return (None, None, None)

        return (hex_value[1], hex_value[2], hex_value[3])

    @staticmethod
    def zip_db(db_path: str, zip_dir: str):
        if not os.path.isfile(db_path):
            raise FileNotFoundError(f'Файл базы данных не найден: {db_path}')
        if not db_path.endswith('.db'):
            raise ValueError(f'Файл не является .db: {db_path}')

        os.makedirs(zip_dir, exist_ok=True)
        filename = os.path.basename(db_path)
        zip_path = os.path.join(zip_dir, filename.replace('.db', '.zip'))

        if os.path.exists(zip_path):
            raise FileExistsError(f'Архив уже существует: {zip_path}.')

        with zipfile.ZipFile(
            zip_path, 'w', compression=zipfile.ZIP_DEFLATED
        ) as zipf:
            zipf.write(db_path, arcname=filename)

        os.remove(db_path)
        print(
            f'БД {filename} архивирована в {zip_path}. Исходный файл удалён.'
        )

    @staticmethod
    def unzip_db(zip_path: str, extract_dir: str, overwrite: bool = False):
        if not os.path.isfile(zip_path):
            raise FileNotFoundError(f'Архив не найден: {zip_path}')
        if not zip_path.endswith('.zip'):
            raise ValueError(f'Файл не является .zip архивом: {zip_path}')

        os.makedirs(extract_dir, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zipf:
            for member in zipf.namelist():
                target_path = os.path.join(extract_dir, member)

                if os.path.exists(target_path) and not overwrite:
                    raise FileExistsError(
                        f'Файл {target_path} уже существует.')

            zipf.extractall(path=extract_dir)

        if overwrite:
            os.remove(zip_path)
            print(f'Архив {zip_path} распакован в {extract_dir} и удален.')
        else:
            print(f'Архив {zip_path} распакован в {extract_dir}.')

    def data_not_in_db(self) -> list[str]:
        """
        Поиск файлов .csv или .gz, не вошедших в БД.
        Если для месяца уже есть zip-архив, тогда этот архив будет
        разархивирован для проверки данных.
        Файлы, относящиеся к месяцам старше Config.MONTH_AGO месяцев назад,
        пропускаются.
        """
        unprocessed_files = []
        db_date_ranges: dict[
            tuple[int, int], tuple[dt.datetime, dt.datetime]
        ] = {}
        cutoff_date = dt.datetime.now() - relativedelta(
            months=Config.MONTH_AGO)

        for filename in os.listdir(Config.DATA_DIR):
            if not filename.startswith(f'{Config.DB_PREFIX}_'):
                continue
            if not (filename.endswith('.db') or filename.endswith('.zip')):
                continue

            file_prefix = filename.replace('.zip', '').replace('.db', '')
            parts = file_prefix.split('_')
            if len(parts) < 4:
                continue

            try:
                year = int(parts[2])
                month = int(parts[3])
            except ValueError:
                continue

            file_month_date = dt.datetime(year, month, 1)
            if file_month_date <= cutoff_date:
                continue

            db_path = os.path.join(Config.DATA_DIR, filename)
            self.switch_database(db_path)
            min_ts, max_ts = self.border_timestamp
            if min_ts and max_ts:
                db_date_ranges[(year, month)] = (min_ts, max_ts)

        for filename in os.listdir(self.STATISTIC_DIR):
            if filename.endswith('.csv.gz'):
                continue
            if not (filename.endswith('.csv') or filename.endswith('.gz')):
                continue

            try:
                file_date = dt.datetime.strptime(filename[:10], '%Y-%m-%d')
                year, month = file_date.year, file_date.month
            except ValueError:
                continue

            file_month_date = dt.datetime(year, month, 1)
            if file_month_date <= cutoff_date:
                continue

            file_path = os.path.join(self.STATISTIC_DIR, filename)
            if (year, month) in db_date_ranges:
                min_ts, max_ts = db_date_ranges[(year, month)]
                if not (min_ts.date() <= file_date.date() < max_ts.date()):
                    unprocessed_files.append(file_path)
            else:
                unprocessed_files.append(file_path)

        return unprocessed_files

    def read_statistics(self, file_path: str) -> pd.DataFrame:
        """Чтение содержимого .csv файла (в т.ч. из gzip архива)"""
        zip_file: bool = file_path.endswith('.gz')
        open_func = gzip.open if zip_file else open

        def line_generator() -> Iterator[list]:
            current_time: dt.datetime | None = None
            with open_func(file_path, 'rt' if zip_file else 'r') as file:
                for line in file:
                    line = line.strip()
                    if line.startswith('T'):
                        current_time = dt.datetime.strptime(
                            line[2:], '%d.%m.%Y_%H:%M:%S'
                        )
                    elif current_time:
                        values = line[2:].split(',')
                        yield [current_time] + values

        return pd.DataFrame(
            line_generator(),
            columns=[
                'timestamp', 'modem_ip', 'mac', 'local_id',
                'voltage_1', 'current_1', 'angle_1',
                'voltage_2', 'current_2', 'angle_2',
                'voltage_3', 'current_3', 'angle_3',
            ]
        )

    @staticmethod
    def hex_to_bytes(hex_str: str) -> bytes | None:
        if pd.isna(hex_str) or hex_str == '':
            return None
        try:
            return bytes.fromhex(hex_str)
        except ValueError:
            return None

    def prepare_statistic_from_row(self, row: Series) -> Statistic:
        """Преобразование данных ряда в Dataframe в Static."""
        return Statistic(
            timestamp=row.timestamp,
            modem_ip=row.modem_ip,
            mac=row.mac,
            local_id=int(row.local_id),

            voltage_1=self.hex_to_bytes(row.voltage_1),
            current_1=self.hex_to_bytes(row.current_1),
            angle_1=self.hex_to_bytes(row.angle_1),

            voltage_2=self.hex_to_bytes(row.voltage_2),
            current_2=self.hex_to_bytes(row.current_2),
            angle_2=self.hex_to_bytes(row.angle_2),

            voltage_3=self.hex_to_bytes(row.voltage_3),
            current_3=self.hex_to_bytes(row.current_3),
            angle_3=self.hex_to_bytes(row.angle_3),
        )

    def statistics_2_db(self):
        """Запись статистики из .gz и .csv по БД распределенным по месяцам."""
        batch_size = 100_000
        data_not_in_db = self.data_not_in_db()

        for index, file_path in enumerate(data_not_in_db):
            print(f'Файл {file_path} ({index + 1}/{len(data_not_in_db)})')
            df = self.read_statistics(file_path)
            total = len(df)

            for start in range(0, total, batch_size):
                end = min(start + batch_size, total)
                batch_df = df.iloc[start:end]

                statistics = []
                for i, row in enumerate(batch_df.itertuples(index=False)):
                    progress_bar(
                        start + i, total,
                        f'Подготовка {file_path} для записи в БД: '
                    )
                    stat = self.prepare_statistic_from_row(row)
                    statistics.append(stat)

                self.add_statistics_to_monthly_db(statistics)
