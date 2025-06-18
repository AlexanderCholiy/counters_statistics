from datetime import datetime
from typing import Callable, TypeVar

from colorama import Fore, Style


T = TypeVar('T')


def execution_time(func: Callable[..., T]) -> Callable[..., T]:
    def wrapper(*args: tuple, **kwargs: dict) -> T:
        start_time = datetime.now()
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            execution_time = datetime.now() - start_time
            total_seconds = execution_time.total_seconds()
            base_msg = (
                f'{Fore.LIGHTBLUE_EX}Функция '
                f'{Style.RESET_ALL}'
                f'{func.__name__} '
                f'{Fore.LIGHTBLUE_EX}выполнялась '
                f'{Style.RESET_ALL}'
            )

            if total_seconds >= 60:
                minutes = int(total_seconds // 60)
                seconds = total_seconds % 60
                msg = f'{base_msg}{minutes} мин. {round(seconds, 2)} сек.'
            elif total_seconds >= 1:
                seconds = round(total_seconds, 2)
                msg = f'{base_msg}{seconds} сек.'
            else:
                milliseconds = round(execution_time.microseconds / 1000, 2)
                msg = f'{base_msg}{milliseconds} мс.'

            print(msg + Style.RESET_ALL)

    return wrapper
