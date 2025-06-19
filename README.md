# Сбор и обработка статистики счётчиков

## 📌 Описание

Утилита для работы со статистикой счётчиков:
- Разделяет большую БД по месяцам.
- Сохраняет данные счётчиков в Excel по IP-адресу за указанный период.
- Архивирует старые базы данных и очищает директорию.
- Импортировать статистику из CSV/GZ-файлов в базу данных.

Проект построен с акцентом на производительность, логгирование, удобную аргументацию и расширяемость.

## 🖥️ Автоматизация через crontab

1. **Сделайте скрипт запуска исполняемым:**
```bash
sudo chmod +x run_counters_statistics.sh
```

2. **Для автоматического выполнения задач на сервере рекомендуется использовать системный crontab:**
# Удаление обработанных CSV в 00:00
0 0 * * * a.choliy /home/a.choliy/counters_statistics/run_counters_statistics.sh --remove_processed_csv_gz

# Загрузка статистики в БД в 01:00
0 5 * * * a.choliy /home/a.choliy/counters_statistics/run_counters_statistics.sh --statistics_2_db

# Архивация старых БД в 02:00
0 6 * * * a.choliy /home/a.choliy/counters_statistics/run_counters_statistics.sh --zip_and_remove_old_dbs
