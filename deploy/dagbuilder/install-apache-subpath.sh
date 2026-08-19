#!/usr/bin/env bash
#
# Отдать конструктор по адресу https://airflow-test.oms66.ru/etl_configurator/
# БЕЗ сисадминов: ни DNS-заявки, ни нового сертификата — только root на сервере.
#
#   sudo bash deploy/dagbuilder/install-apache-subpath.sh
#
# Переменные (все необязательные):
#   VHOST=/etc/apache2/sites-available/airflow-test.conf   файл vhost
#   SERVER_NAME=airflow-test.oms66.ru                      как его найти
#   URLPATH=/etl_configurator                              путь на домене
#   PORT=8085                                              порт сервиса
#   AUTH_USER=konkin                                       первый пользователь
#   NO_AUTH=1                                              БЕЗ авторизации (см. ниже)
#
# Скрипт идемпотентный: повторный запуск заменяет свой блок, чужого не трогает.
# Перед правкой делается копия vhost, после — configtest; не прошёл — откат.

set -uo pipefail

SERVER_NAME="${SERVER_NAME:-airflow-test.oms66.ru}"
URLPATH="${URLPATH:-/etl_configurator}"
PORT="${PORT:-8085}"
AUTH_FILE="${AUTH_FILE:-/etc/apache2/etl_configurator.htpasswd}"
UNIT="etl-dagbuilder-api"
MARK_BEGIN="# >>> etl_configurator (конструктор ETL-линий) — вставлено install-apache-subpath.sh"
MARK_END="# <<< etl_configurator"

ok()   { printf '  \033[32m✓\033[0m %s\n' "$1"; }
bad()  { printf '  \033[31m✗\033[0m %s\n' "$1"; exit 1; }
warn() { printf '  \033[33m!\033[0m %s\n' "$1"; }

[ "$(id -u)" -eq 0 ] || bad "нужен root: sudo bash $0"
command -v apache2ctl >/dev/null || bad "не нашёл apache2ctl — это точно тот сервер?"
command -v python3    >/dev/null || bad "нужен python3 (только для аккуратной вставки в конфиг)"

echo "== Что настраиваем =="
echo "  адрес:  https://$SERVER_NAME$URLPATH/"
echo "  сервис: 127.0.0.1:$PORT"
echo

# ── 1. сервис должен уже работать ────────────────────────────────────────────
# Настроить Apache на мёртвый порт — значит получить 503 и полчаса искать
# причину не там, где она есть.
echo "== Сервис =="
if systemctl is-active --quiet "$UNIT"; then
  ok "$UNIT работает"
else
  bad "$UNIT не запущен — сначала: sudo bash deploy/dagbuilder/install.sh"
fi
code=$(curl -s -o /dev/null -w '%{http_code}' --noproxy '*' "http://127.0.0.1:$PORT/api/git/status")
[ "$code" = "200" ] && ok "API отвечает на 127.0.0.1:$PORT" \
                    || bad "API на 127.0.0.1:$PORT не отвечает (код '$code')"

# ── 2. находим vhost ─────────────────────────────────────────────────────────
echo
echo "== Конфиг Apache =="

# Спрашиваем у САМОГО Apache, а не ищем по каталогам. `apache2ctl -S` печатает
# карту виртуальных хостов с файлом и номером строки — и это единственный
# надёжный способ: конфиг может лежать где угодно (sites-enabled, conf-enabled,
# conf.d, свой Include), а grep по одному каталогу такую раскладку не находит и
# врёт, будто сайта нет, — при том что сайт открыт в соседней вкладке.
CTL=$(command -v apache2ctl || command -v apachectl)
VHOST_MAP=$("$CTL" -S 2>&1)

if [ -n "${VHOST:-}" ]; then
  [ -f "$VHOST" ] || bad "нет файла $VHOST"
else
  # Разбор карты. Строки вида: *:443  имя (/etc/…/x.conf:1). Берём только
  # :443 — на :80 обычно висит редирект, и правь мы его, ничего бы не заработало.
  # Тонкость: у Apache два формата вывода, и в частом (Debian,
  # NameVirtualHost) порт стоит НЕ в строке с именем, а в заголовке секции:
  #
  #   *:443                  is a NameVirtualHost
  #            port 443 namevhost airflow-test.oms66.ru (/etc/…/x.conf:1)
  #
  # Поэтому порт запоминаем с заголовка и тянем вниз, а не ищем в той же
  # строке. Только match() с двумя аргументами и RSTART/RLENGTH — это POSIX и
  # работает в mawk, который в Debian стоит вместо gawk по умолчанию.
  parse_map() {
    awk -v name="$SERVER_NAME" -v want="$1" '
      # заголовок секции: начинается не с пробела и содержит :ЧИСЛО
      /^[^ \t]/ && /:[0-9]+/ {
        p = $0
        sub(/^[^:]*:/, "", p)
        sub(/[^0-9].*$/, "", p)
        if (p != "") port = p
      }
      index($0, name) > 0 {
        # в строке с именем порт бывает и свой: "port 443 namevhost ..."
        this = port
        if (match($0, /port[ \t]+[0-9]+/)) {
          s = substr($0, RSTART, RLENGTH); sub(/[^0-9]*/, "", s); this = s
        }
        if (want == "" || this == want) {
          if (match($0, /\([^)]*:[0-9]+\)/)) {
            f = substr($0, RSTART + 1, RLENGTH - 2)
            sub(/:[0-9]+$/, "", f)
            print f
          }
        }
      }
    ' <<<"$VHOST_MAP" | sort -u
  }

  mapfile -t found < <(parse_map 443)
  # Если по :443 не нашлось — вдруг сайт живёт на другом порту; смотрим шире.
  if [ "${#found[@]}" -eq 0 ]; then
    mapfile -t found < <(parse_map "")
    [ "${#found[@]}" -gt 0 ] && warn "vhost найден, но не на :443 — проверьте порт"
  fi

  case ${#found[@]} in
    1) VHOST="${found[0]}" ;;
    0)
      echo
      echo "  Apache не знает виртуального хоста '$SERVER_NAME'. Вот что он знает:"
      sed 's/^/     /' <<<"$VHOST_MAP"
      echo
      echo "  Дальше одно из трёх:"
      echo "   * имя в конфиге записано иначе (ServerAlias, другой регистр) —"
      echo "     запустите с SERVER_NAME='<как в списке выше>';"
      echo "   * нужный файл виден в списке — укажите его: VHOST=/путь/к/файлу.conf;"
      echo "   * этот сайт отдаёт ДРУГАЯ машина (Apache здесь только локальный)."
      echo "     Проверьте, куда он резолвится и сюда ли приходит:"
      echo "       getent hosts $SERVER_NAME ; hostname -I"
      bad "не нашёл, какой vhost править"
      ;;
    *)
      printf '     %s\n' "${found[@]}"
      bad "таких vhost несколько — укажите нужный через VHOST=..."
      ;;
  esac
fi
ok "vhost: $VHOST"

grep -q '<VirtualHost \*:443>' "$VHOST" || warn "в файле нет <VirtualHost *:443> — проверьте вручную"

# ── 3. модули ────────────────────────────────────────────────────────────────
mods="proxy proxy_http headers"
[ -n "${NO_AUTH:-}" ] || mods="$mods auth_basic authn_file authz_user"
for m in $mods; do
  if apache2ctl -M 2>/dev/null | grep -q "^ ${m}_module"; then
    ok "модуль $m уже включён"
  else
    a2enmod "$m" >/dev/null 2>&1 && ok "модуль $m включён" || bad "не удалось включить модуль $m"
  fi
done

# ── 4. авторизация ───────────────────────────────────────────────────────────
echo
echo "== Доступ =="
if [ -n "${NO_AUTH:-}" ]; then
  warn "NO_AUTH=1 — путь будет открыт ВСЕМ, кто дотянется до $SERVER_NAME."
  warn "Конструктор пишет в репозиторий, делает git push и ходит в боевые БД."
  warn "Своей авторизации у него нет: форма входа airflow его НЕ закрывает."
else
  if [ -s "$AUTH_FILE" ]; then
    ok "файл паролей уже есть: $AUTH_FILE ($(wc -l <"$AUTH_FILE") польз.)"
  else
    command -v htpasswd >/dev/null || bad "нет htpasswd — поставьте apache2-utils (или NO_AUTH=1)"
    user="${AUTH_USER:-}"
    if [ -z "$user" ]; then
      read -rp "  имя первого пользователя: " user
    fi
    [ -n "$user" ] || bad "пустое имя пользователя"
    htpasswd -c "$AUTH_FILE" "$user" || bad "htpasswd не отработал"
    chmod 640 "$AUTH_FILE"; chgrp www-data "$AUTH_FILE" 2>/dev/null
    ok "создан $AUTH_FILE, пользователь $user"
    echo "     ещё людей: sudo htpasswd $AUTH_FILE <имя>   (без -c!)"
  fi
fi

# ── 5. правим vhost ──────────────────────────────────────────────────────────
echo
echo "== Правка vhost =="
BACKUP="$VHOST.bak-$(date +%Y%m%d-%H%M%S)"
cp -a "$VHOST" "$BACKUP"
ok "копия: $BACKUP"

AUTH_BLOCK=""
[ -n "${NO_AUTH:-}" ] || AUTH_BLOCK=$(cat <<EOF

    <Location $URLPATH/>
        AuthType Basic
        AuthName "Konstruktor ETL"
        AuthUserFile $AUTH_FILE
        Require valid-user
    </Location>
EOF
)

BLOCK=$(cat <<EOF
    $MARK_BEGIN
    # Без завершающего слэша страница откроется ПУСТОЙ: адрес API и пути к
    # файлам сборки приложение считает от document.baseURI, и на $URLPATH
    # базой становится корень сайта — запросы уходят в /api/ (то есть в
    # airflow), а скрипты в /assets/, где их нет.
    RedirectMatch permanent ^$URLPATH\$ $URLPATH/

    ProxyPass        $URLPATH/  http://127.0.0.1:$PORT/
    ProxyPassReverse $URLPATH/  http://127.0.0.1:$PORT/$AUTH_BLOCK
    $MARK_END

EOF
)

MARK_BEGIN="$MARK_BEGIN" MARK_END="$MARK_END" BLOCK="$BLOCK" VHOST="$VHOST" python3 - <<'PY'
import os, re, sys

path = os.environ["VHOST"]
begin, end, block = os.environ["MARK_BEGIN"], os.environ["MARK_END"], os.environ["BLOCK"]
text = open(path, encoding="utf-8").read()

# Повторный запуск: свой прежний блок вырезаем целиком, чужое не трогаем.
# Заодно убираем блок под ПРЕЖНИМ именем пути (etl_builder): если его успели
# поставить, два ProxyPass на разные пути к одному сервису не поломают Apache,
# но оставят в конфиге мусор, про который через месяц никто не вспомнит.
removed = 0
for b, e in ((begin, end),
             ("# >>> etl_builder ", "# <<< etl_builder")):
    pattern = re.compile(r"[ \t]*" + re.escape(b) + r".*?" + re.escape(e) + r"\n", re.S)
    text, n = pattern.subn("", text)
    removed += n

# Наш ProxyPass обязан стоять ВЫШЕ общего `ProxyPass /`: Apache берёт первое
# подходящее правило, и `/` подходит под всё. Ставим прямо перед ним.
m = re.search(r"^[ \t]*ProxyPass[ \t]+/[ \t]", text, re.M)
if m:
    at = m.start()
else:
    # общего ProxyPass нет — тогда просто в конец нужного VirtualHost
    m = re.search(r"^[ \t]*</VirtualHost>", text, re.M)
    if not m:
        sys.exit("не нашёл, куда вставить: нет ни 'ProxyPass /', ни </VirtualHost>")
    at = m.start()

open(path, "w", encoding="utf-8").write(text[:at] + block + text[at:])
print(f"    (прежний блок удалён: {removed}); вставлено перед позицией {at}")
PY
[ $? -eq 0 ] || { cp -a "$BACKUP" "$VHOST"; bad "вставка не удалась — конфиг возвращён из копии"; }
ok "блок вставлен перед общим ProxyPass /"

# ── 6. проверка и применение ─────────────────────────────────────────────────
echo
echo "== Проверка конфига =="
if out=$(apache2ctl configtest 2>&1); then
  ok "configtest: ${out##*$'\n'}"
else
  echo "$out" | sed 's/^/     /'
  cp -a "$BACKUP" "$VHOST"
  bad "configtest не прошёл — конфиг возвращён из копии, ничего не изменилось"
fi

systemctl reload apache2 || bad "apache2 не перечитал конфиг"
ok "apache2 перечитал конфиг"

echo
echo "== Проверка адреса =="
url="https://$SERVER_NAME$URLPATH/"
code=$(curl -s -k -o /dev/null -w '%{http_code}' "$url")
case "$code" in
  200) ok "$url отвечает 200 (авторизация отключена)" ;;
  401) ok "$url отвечает 401 — Apache просит логин, как и задумано" ;;
  503) warn "503: Apache настроен, но сервис не отвечает — systemctl status $UNIT" ;;
  *)   warn "код $code — проверьте /var/log/apache2/error.log" ;;
esac
code=$(curl -s -k -o /dev/null -w '%{http_code}' "https://$SERVER_NAME$URLPATH")
[ "$code" = "301" ] && ok "адрес без слэша перенаправляется (301)" \
                    || warn "адрес без слэша отдал $code вместо 301 — страница может открыться пустой"

echo
echo "Готово. Открывайте: $url"
[ -n "${NO_AUTH:-}" ] || echo "Добавить людей: sudo htpasswd $AUTH_FILE <имя>"
echo "Откатить:  sudo cp -a $BACKUP $VHOST && sudo systemctl reload apache2"
