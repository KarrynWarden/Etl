"""Базовые пути проекта.

FULL_PATH — корень airflow (с завершающим '/'). MODE — суффикс среды
(Prod / Test / "" для dev): добавляется к 'etlFolder', чтобы prod и test
могли жить рядом без конфликтов.

Хардкод до тех пор, пока на сервере не появится python-dotenv.
"""

FULL_PATH = "/opt/airflow/airflow/"
MODE = "Prod"
