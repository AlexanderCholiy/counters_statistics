import os
import datetime as dt
from collections import defaultdict

import pandas as pd
from sqlalchemy import create_engine, inspect, MetaData, tuple_, func
from sqlalchemy.orm import sessionmaker

from .models import Statistic, Base
from .config import Config


class CountersStatisticDB(Config):

    def __init__(self, db_path: str | None = None, debug: bool = False):
        self.debug = debug
        self.engine = self.create_engine(db_path)
        self.metadata = MetaData()
        self.inspector = inspect(self.engine)
        self.session = sessionmaker(bind=self.engine)

    def create_engine(self, db_path: str = None):
        """Создание engine для указанной базы данных или по умолчанию"""
        if db_path:
            return create_engine(f'sqlite:///{db_path}', echo=self.debug)
        return create_engine(f'sqlite:///{self.DB_PATH}', echo=self.debug)

    def switch_database(self, db_path: str):
        """Переключение на другую базу данных"""
        self.engine = self.create_engine(db_path)
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
        db_name = f'counters_statistics_{year}_{month:02d}.db'
        db_path = os.path.join(self.DATA_DIR, db_name)
        engine = create_engine(f'sqlite:///{db_path}', echo=self.debug)
        return engine

    @staticmethod
    def str_to_bytes(s: str | None) -> bytes | None:
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
        return tuple(byte_data) if byte_data else (None, None, None)
