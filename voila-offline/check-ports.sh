#!/usr/bin/env bash
#
# Проверка занятости TCP-портов — чтобы не поднять сервис на уже занятом порту.
#
# Использование:
#   bash check-ports.sh                # типовые порты проекта + предложить свободный
#   bash check-ports.sh 8083           # проверить конкретный порт
#   bash check-ports.sh 8083 9000      # несколько портов
#   bash check-ports.sh 8083-8099      # диапазон
#
# Работает без root (через ss/netstat, либо через попытку bind на Python).
# Имя процесса, занявшего порт, показывается только с правами (sudo/ss -p).

set -uo pipefail

# Известные порты проекта — для подсказки в выводе
declare -A KNOWN=(
  [8080]="airflow prod"
  [8081]="JupyterLab"
  [8082]="airflow-test"
  [8083]="Voila (конструктор дагов)"
)
SUGGEST_FROM=8083
SUGGEST_TO=8099

have() { command -v "$1" >/dev/null 2>&1; }

# Множество слушающих (LISTEN) TCP-портов, если доступны ss/netstat
LISTEN_SET=""
_have_list=0
if have ss; then
  LISTEN_SET=$(ss -ltnH 2>/dev/null | awk '{print $4}' | sed -E 's/.*:([0-9]+)$/\1/' | sort -un)
  _have_list=1
elif have netstat; then
  LISTEN_SET=$(netstat -ltn 2>/dev/null | awk 'NR>2{print $4}' | sed -E 's/.*[:.]([0-9]+)$/\1/' | sort -un)
  _have_list=1
fi

# 0 = занят, 1 = свободен
is_busy() {
  local p=$1
  if [ "$_have_list" -eq 1 ]; then
    grep -qx "$p" <<<"$LISTEN_SET"
    return $?
  fi
  # fallback: пробуем занять порт сами
  python3 - "$p" <<'PY' 2>/dev/null
import socket, sys
p = int(sys.argv[1])
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    s.bind(("0.0.0.0", p)); code = 1     # удалось занять -> свободен
except OSError:
    code = 0                              # не удалось -> занят
finally:
    s.close()
sys.exit(code)
PY
}

# что слушает порт (нужны права для имени процесса)
who() {
  have ss && ss -ltnp "sport = :$1" 2>/dev/null | awk 'NR>1'
}

check_one() {
  local p=$1 label=${KNOWN[$p]:-}
  if is_busy "$p"; then
    printf "  %-6s ЗАНЯТ    %s\n" "$p" "${label:+($label)}"
    local w; w=$(who "$p")
    [ -n "$w" ] && sed 's/^/            /' <<<"$w"
  else
    printf "  %-6s свободен %s\n" "$p" "${label:+($label)}"
  fi
}

# развернуть аргумент "8083-8099" в список портов
expand() {
  if [[ "$1" == *-* ]]; then
    seq "${1%-*}" "${1#*-}"
  else
    echo "$1"
  fi
}

echo "== Проверка портов =="
if [ "$_have_list" -eq 0 ]; then
  echo "(ss/netstat недоступны — проверяю через попытку bind на Python)"
fi

if [ "$#" -eq 0 ]; then
  # типовые порты проекта
  for p in $(printf '%s\n' "${!KNOWN[@]}" | sort -n); do check_one "$p"; done
  echo
  # предложить первый свободный
  free=""
  for p in $(seq "$SUGGEST_FROM" "$SUGGEST_TO"); do
    if ! is_busy "$p"; then free=$p; break; fi
  done
  if [ -n "$free" ]; then
    echo "Свободный порт для сервиса: $free"
    echo "  (в etl-dagbuilder.service -> ExecStart: --port=$free)"
  else
    echo "В диапазоне $SUGGEST_FROM-$SUGGEST_TO свободных нет — задай другой."
  fi
else
  for a in "$@"; do
    for p in $(expand "$a"); do check_one "$p"; done
  done
fi
