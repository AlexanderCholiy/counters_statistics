from sqlalchemy import (
    Column, Integer, String, DateTime, BLOB, UniqueConstraint
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class Statistic(Base):
    __tablename__ = 'statistic'

    id = Column(Integer, primary_key=True, nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)
    modem_ip = Column(String(length=32), nullable=False, index=True)
    mac = Column(String(length=32), nullable=False, index=True)
    local_id = Column(Integer, nullable=False)
    voltage_1 = Column(BLOB, nullable=True)
    current_1 = Column(BLOB, nullable=True)
    angle_1 = Column(BLOB, nullable=True)
    voltage_2 = Column(BLOB, nullable=True)
    current_2 = Column(BLOB, nullable=True)
    angle_2 = Column(BLOB, nullable=True)
    voltage_3 = Column(BLOB, nullable=True)
    current_3 = Column(BLOB, nullable=True)
    angle_3 = Column(BLOB, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            'timestamp', 'modem_ip', 'mac', 'local_id', name='unique_statistic'
        ),
    )

    def __str__(self):
        return (
            f'{self.timestamp} - {self.modem_ip} - '
            f'{self.mac} - {self.local_id}'
        )
