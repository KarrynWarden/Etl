#!/usr/bin/env bash
#
# Офлайн-установка Voilà в окружение Jupyter (для веб-конструктора ETL-дагов).
#
#   PYTHON=/opt/jupyter/bin/python bash install.sh
#
# Ставит Voilà и недостающие зависимости ТОЛЬКО из локальной папки ./wheels
# (--no-index, без интернета). Уже установленные в Jupyter пакеты (jupyter_server,
# nbconvert, pyzmq и т.п.) НЕ трогаются — pip берёт их как есть.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WHEELS_DIR="${SCRIPT_DIR}/wheels"
[ -d "${WHEELS_DIR}" ] || { echo "Нет папки с wheel'ами: ${WHEELS_DIR}" >&2; exit 1; }

pick_python() {
  if [ -n "${PYTHON:-}" ]; then echo "${PYTHON}"; return; fi
  for c in /opt/jupyter/bin/python /opt/jupyter/bin/python3 python3 python; do
    command -v "$c" >/dev/null 2>&1 && { echo "$c"; return; }
  done
}
PY="$(pick_python)"
[ -n "${PY}" ] || { echo "Не найден интерпретатор Python." >&2; exit 1; }

if [ "$("${PY}" -c 'import sys;print(sys.version_info[0])')" != "3" ]; then
  echo "'${PY}' — не Python 3. Укажи python окружения Jupyter:" >&2
  echo "  PYTHON=/opt/jupyter/bin/python bash install.sh" >&2
  exit 1
fi

echo ">>> Интерпретатор: ${PY}"
"${PY}" --version
"${PY}" -m pip --version
echo

echo ">>> Ставлю Voilà офлайн из ${WHEELS_DIR} (без обновления существующих пакетов)"
"${PY}" -m pip install --no-index --find-links="${WHEELS_DIR}" voila

echo
echo ">>> Проверка:"
"${PY}" -m voila --version && echo "Voilà установлен."
echo
echo "Дальше — см. README.md: настрой сервис (etl-dagbuilder.service) и открой"
echo "конструктор по адресу http://<сервер>:8083/"
