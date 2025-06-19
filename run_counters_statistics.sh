#!/bin/bash

# Путь до директории скрипта
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Активация виртуального окружения
source "$SCRIPT_DIR/venv/bin/activate"

# Запуск Python-скрипта с аргументами, переданными в bash-скрипт
python "$SCRIPT_DIR/counters_statistics.py" "$@"