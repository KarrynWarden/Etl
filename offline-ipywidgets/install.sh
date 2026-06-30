#!/usr/bin/env bash
#
# Офлайн-установка ipywidgets на сервер Jupyter без доступа в интернет.
#
# Использование:
#   bash install.sh
#
# Скрипт ставит ipywidgets и зависимости ТОЛЬКО из локальной папки ./wheels,
# не обращаясь в сеть (--no-index). Уже установленные подходящие пакеты
# (ipython, traitlets и т.п.) трогаться не будут.

set -euo pipefail

# Папка со скриптом, чтобы install.sh можно было запускать из любого места
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WHEELS_DIR="${SCRIPT_DIR}/wheels"

if [ ! -d "${WHEELS_DIR}" ]; then
  echo "ОШИБКА: не найдена папка с wheel-файлами: ${WHEELS_DIR}" >&2
  exit 1
fi

echo ">>> Python:"
python -m pip --version
echo

echo ">>> Устанавливаю ipywidgets офлайн из ${WHEELS_DIR}"
python -m pip install --no-index --find-links="${WHEELS_DIR}" ipywidgets

echo
echo ">>> Готово. Проверка:"
python -c "import ipywidgets; print('ipywidgets', ipywidgets.__version__)"

echo
echo "Теперь перезапустите ядро/сервер JupyterLab, чтобы виджеты заработали."
