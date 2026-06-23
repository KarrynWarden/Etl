#!/usr/bin/env bash
#
# Запускать НА СЕРВЕРЕ (ssh devel@airflow, затем sudo -s).
#
# Упаковывает в один tar.gz всё, что нужно для локального airflow на dev-PC:
#   /opt/python3.10     — базовый интерпретатор (на него ссылается venv)
#   /opt/airflow/venv   — airflow 2.7.2 + все зависимости (cx_Oracle, psycopg2, ...)
#   /opt/oracle         — Oracle Instant Client + config
#
# Пути в архиве относительные от "/", поэтому на dev-PC распаковка идёт в "/"
# и всё ложится по тем же путям — venv остаётся рабочим без правок.
#
set -euo pipefail

OUT="/home/devel/airflow-runtime-$(date +%Y%m%d).tar.gz"

for d in /opt/python3.10 /opt/airflow/venv /opt/oracle; do
    [[ -e "$d" ]] || { echo "ОШИБКА: нет $d" >&2; exit 1; }
done

echo "Упаковываю (это ~1 ГБ, займёт минуту)..."
tar czf "$OUT" -C / \
    opt/python3.10 \
    opt/airflow/venv \
    opt/oracle

chown devel:devel "$OUT" 2>/dev/null || true

echo "Готово: $OUT  ($(du -h "$OUT" | cut -f1))"
echo
echo "Теперь с dev-PC (без sudo — /opt на Astra SE закрыт даже root'у):"
echo "  scp devel@airflow:$OUT ~/"
echo "  mkdir -p ~/airflow-runtime && tar xzf ~/$(basename "$OUT") -C ~/airflow-runtime"
echo "  bash local/relocate-venv.sh ~/airflow-runtime"
