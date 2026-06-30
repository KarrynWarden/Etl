#!/usr/bin/env bash
#
# Офлайн-установка ipywidgets на сервер Jupyter без доступа в интернет.
#
# Использование:
#   bash install.sh
#
# Если на сервере несколько Python (например, системный python = 2.7),
# укажите нужный интерпретатор явно через переменную PYTHON:
#   PYTHON=/opt/jupyter/bin/python bash install.sh
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

# Подбираем интерпретатор Python 3 для окружения Jupyter.
# Приоритет: переменная PYTHON -> python окружения Jupyter -> python3 -> python.
pick_python() {
  if [ -n "${PYTHON:-}" ]; then echo "${PYTHON}"; return; fi
  for cand in /opt/jupyter/bin/python /opt/jupyter/bin/python3 python3 python; do
    if command -v "${cand}" >/dev/null 2>&1; then echo "${cand}"; return; fi
  done
}

PY="$(pick_python)"
if [ -z "${PY}" ]; then
  echo "ОШИБКА: не найден интерпретатор Python." >&2
  exit 1
fi

# Wheel-файлы собраны под Python 3. Если выбран Python 2 — это ошибка окружения.
PYMAJOR="$("${PY}" -c 'import sys; print(sys.version_info[0])')"
if [ "${PYMAJOR}" != "3" ]; then
  echo "ОШИБКА: '${PY}' — это Python ${PYMAJOR}, а нужен Python 3." >&2
  echo "Укажите интерпретатор Jupyter явно, например:" >&2
  echo "  PYTHON=/opt/jupyter/bin/python bash install.sh" >&2
  exit 1
fi

echo ">>> Использую интерпретатор: ${PY}"
"${PY}" --version
"${PY}" -m pip --version
echo

echo ">>> Устанавливаю ipywidgets офлайн из ${WHEELS_DIR}"
"${PY}" -m pip install --no-index --find-links="${WHEELS_DIR}" ipywidgets

echo
echo ">>> Готово. Проверка:"
"${PY}" -c "import ipywidgets; print('ipywidgets', ipywidgets.__version__)"

echo
echo "Теперь перезапустите ядро/сервер JupyterLab, чтобы виджеты заработали."
