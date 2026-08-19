#!/usr/bin/env bash
#
# Постоянная установка веб-конструктора ETL-линий на сервере.
#
#   sudo bash deploy/dagbuilder/install.sh
#
# Что делает: проверяет предпосылки, ставит systemd-юнит, включает его и
# проверяет, что сервис реально отвечает. Apache и DNS НЕ трогает — это работа
# сисадминов, см. README.md рядом.
#
# Скрипт идемпотентный: повторный запуск ничего не ломает.

set -uo pipefail

REPO="${REPO:-/opt/jupyter/workdir/konkin/etl}"
PYTHON="${PYTHON:-/opt/jupyter/bin/python}"
PORT="${PORT:-8085}"
UNIT="etl-dagbuilder-api"
RUNAS="${RUNAS:-jupyter}"

ok()   { printf '  \033[32m✓\033[0m %s\n' "$1"; }
bad()  { printf '  \033[31m✗\033[0m %s\n' "$1"; FAILED=1; }
warn() { printf '  \033[33m!\033[0m %s\n' "$1"; }
FAILED=0

echo "== Предпосылки =="

[ -d "$REPO" ] && ok "репозиторий: $REPO" || bad "нет каталога $REPO (задайте REPO=...)"
[ -x "$PYTHON" ] && ok "python: $PYTHON" || bad "нет $PYTHON (задайте PYTHON=...)"

# Фронтенд обязан быть собран И целым. Разъехавшиеся index.html и assets/ дают
# ПУСТУЮ белую страницу без единого сообщения — самый дорогой способ узнать об
# ошибке, потому что выглядит он как «сервис не работает».
if [ -x "$PYTHON" ] && [ -d "$REPO" ]; then
  if out=$("$PYTHON" - "$REPO" <<'PY' 2>&1
import sys, os
sys.path.insert(0, sys.argv[1])
from tools.dagbuilder_api import _dist_problems
problems = _dist_problems()
print("\n".join(problems) if problems else "ok")
PY
  ); then
    [ "$out" = "ok" ] && ok "собранный фронтенд на месте и целый" \
                      || bad "фронтенд: $out"
  else
    bad "не удалось проверить фронтенд: $out"
  fi

  # Самопроверка API — без БД и без сети. Ловит и битый конфиг в репозитории.
  if "$PYTHON" "$REPO/tools/dagbuilder_api.py" --selftest >/dev/null 2>&1; then
    ok "самопроверка API проходит"
  else
    bad "самопроверка API падает: $PYTHON $REPO/tools/dagbuilder_api.py --selftest"
  fi
fi

# git push должен работать ПОД ПОЛЬЗОВАТЕЛЕМ СЕРВИСА, а не под тем, кто ставит.
# Иначе кнопка «Записать и запушить» будет падать уже в проде — на человеке,
# который к настройке отношения не имеет.
if id "$RUNAS" >/dev/null 2>&1; then
  ok "пользователь сервиса: $RUNAS"
  if sudo -u "$RUNAS" git -C "$REPO" push --dry-run >/dev/null 2>&1; then
    ok "git push из-под $RUNAS работает"
  else
    warn "git push из-под $RUNAS не проходит — кнопка «запушить» будет падать."
    warn "  проверьте: sudo -u $RUNAS git -C $REPO push --dry-run"
  fi
else
  bad "нет пользователя $RUNAS (задайте RUNAS=...)"
fi

# Порт
if ss -ltnH 2>/dev/null | awk '{print $4}' | grep -qE "[:.]$PORT\$"; then
  if systemctl is-active --quiet "$UNIT"; then
    ok "порт $PORT занят самим $UNIT (переустановка)"
  else
    bad "порт $PORT занят кем-то другим — bash voila-offline/check-ports.sh $PORT"
  fi
else
  ok "порт $PORT свободен"
fi

if [ "$FAILED" -ne 0 ]; then
  echo
  echo "Есть незакрытые пункты — исправьте их и запустите снова. Ничего не установлено."
  exit 1
fi

echo
echo "== Установка юнита =="
SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$UNIT.service"
install -m 0644 "$SRC" "/etc/systemd/system/$UNIT.service"
ok "/etc/systemd/system/$UNIT.service"

# Пути и порт в юните — под фактические. Так один файл годится и для теста, и
# для прода, а расхождение «в юните одно, в install.sh другое» невозможно.
sed -i \
  -e "s#^WorkingDirectory=.*#WorkingDirectory=$REPO#" \
  -e "s#^ExecStart=.*#ExecStart=$PYTHON tools/dagbuilder_api.py --host 127.0.0.1 --port $PORT#" \
  -e "s#^User=.*#User=$RUNAS#" \
  "/etc/systemd/system/$UNIT.service"
ok "подставлены REPO=$REPO PORT=$PORT RUNAS=$RUNAS"

systemctl daemon-reload
systemctl enable "$UNIT" >/dev/null 2>&1
systemctl restart "$UNIT"
ok "сервис включён в автозапуск и перезапущен"

echo
echo "== Проверка =="
for i in $(seq 1 15); do
  code=$(curl -s -o /dev/null -w '%{http_code}' --noproxy '*' \
         "http://127.0.0.1:$PORT/api/git/status" 2>/dev/null)
  [ "$code" = "200" ] && break
  sleep 1
done
if [ "$code" = "200" ]; then
  ok "API отвечает: http://127.0.0.1:$PORT/api/git/status"
else
  bad "API не ответил за 15 с (код '$code'). Смотрите: journalctl -u $UNIT -n 50"
fi

code=$(curl -s -o /dev/null -w '%{http_code}' --noproxy '*' "http://127.0.0.1:$PORT/" 2>/dev/null)
[ "$code" = "200" ] && ok "страница отдаётся" || bad "страница не отдаётся (код '$code')"

echo
echo "Дальше — Apache и DNS: см. README.md рядом с этим скриптом."
echo "Пока их нет, проверить можно туннелем:"
echo "  ssh -L 18085:127.0.0.1:$PORT <вы>@<сервер>   →   http://localhost:18085/"
exit "$FAILED"
