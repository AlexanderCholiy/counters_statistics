import re
import shutil
from typing import TypeVar

from colorama import Fore, Style


T = TypeVar('T')


def progress_bar(
    iteration: int,
    total: int,
    message: str = 'Загрузка: ',
    bar_color: str = Fore.LIGHTRED_EX,
) -> None:
    if total == 0:
        return

    terminal_width = shutil.get_terminal_size((80, 20)).columns
    iteration += 1

    percent = round((iteration / total) * 100, 2)
    count_info = f'{percent}% ({iteration}/{total})'
    bar_length = 30
    right_padding: int = 3

    filled_length = int(bar_length * iteration // total)
    bar = (
        f'{bar_color}█' * filled_length
        + f'{Fore.LIGHTBLACK_EX}█' * (bar_length - filled_length)
    )
    bar_display = f'{Fore.BLACK}|{bar}{Fore.BLACK}|'

    left_part = f'{Fore.LIGHTBLUE_EX}{message}{Fore.WHITE}{count_info}'
    left_length = len(strip_ansi(left_part))
    bar_length_real = len(strip_ansi(bar_display))
    padding = terminal_width - left_length - bar_length_real - right_padding
    if padding < 1:
        padding = 1
    spacer = ' ' * padding
    end_space = ' ' * right_padding

    print(f'{left_part}{spacer}{bar_display}{end_space}', end='\r')

    if iteration == total:
        print(Style.RESET_ALL)


def strip_ansi(text: str) -> str:
    """Удаляет ANSI-коды из строки для корректного измерения длины."""
    ansi_escape = re.compile(r'\x1b\[[0-9;]*m')
    return ansi_escape.sub('', text)
