#!/usr/bin/env bash
#
# Перенастраивает venv, распакованный НЕ в /opt (например, в домашнюю папку).
#
# Нужно на Astra SE, где запись в /opt закрыта даже для root мандатным контролем,
# поэтому архив с сервера распаковывается в ~/airflow-runtime. venv помнит старый
# путь /opt/... в симлинках, pyvenv.cfg и шебангах — этот скрипт их перенацеливает.
#
# Использование:
#   tar xzf ~/airflow-runtime-*.tar.gz -C ~/airflow-runtime
#   bash local/relocate-venv.sh ~/airflow-runtime
#
set -euo pipefail

RT="${1:?Укажи папку, куда распакован архив, напр.: ~/airflow-runtime}"
RT="$(cd "$RT" && pwd)"

PYBASE="$RT/opt/python3.10"
VENV="$RT/opt/airflow/venv"

[[ -x "$PYBASE/bin/python3.10" ]] || { echo "Нет $PYBASE/bin/python3.10 — не та папка?" >&2; exit 1; }
[[ -d "$VENV" ]]                  || { echo "Нет $VENV — не та папка?" >&2; exit 1; }

echo "Базовый python: $PYBASE"
echo "venv          : $VENV"

# 1) Симлинки python внутри venv → на перенесённый базовый интерпретатор
ln -sf "$PYBASE/bin/python3.10" "$VENV/bin/python3.10"
ln -sf python3.10 "$VENV/bin/python3"
ln -sf python3.10 "$VENV/bin/python"

# 2) pyvenv.cfg: home/executable указывают на базовый python
if [[ -f "$VENV/pyvenv.cfg" ]]; then
    sed -i \
        -e "s#^home = .*#home = $PYBASE/bin#" \
        -e "s#^executable = .*#executable = $PYBASE/bin/python3.10#" \
        "$VENV/pyvenv.cfg"
fi

# 3) Шебанги в venv/bin: старый /opt/airflow/venv → новый путь
for f in "$VENV"/bin/*; do
    [[ -f "$f" ]] || continue
    if head -c2 "$f" 2>/dev/null | grep -q '#!'; then
        sed -i "1s|^#!/opt/airflow/venv/bin/python.*|#!$VENV/bin/python|" "$f"
    fi
done

# 4) Проверка
echo "── проверка ──"
export LD_LIBRARY_PATH="$PYBASE/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
"$VENV/bin/python" --version
"$VENV/bin/python" "$VENV/bin/airflow" version
echo "OK: venv работает из $VENV"
