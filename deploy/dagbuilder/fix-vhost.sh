#!/usr/bin/env bash
#
# Убрать блок конструктора из vhost и починить конфиг.
#
#   sudo bash deploy/dagbuilder/fix-vhost.sh
#
# Зачем отдельный скрипт: ранние версии install-apache-subpath.sh вставляли
# блок без завершающего перевода строки, и следующая строка приклеивалась к
# закрывающему комментарию:
#
#     # <<< etl_configurator</VirtualHost>
#
# Строка становилась частью комментария и исчезала из конфига. Если это был
# `</VirtualHost>` — Apache говорит «<VirtualHost> was not closed» и показывает
# на первую строку файла, то есть ровно туда, где ничего не сломано.
#
# Здесь блок вырезается целиком, а приклеенный хвост возвращается на свою
# строку. Ставить блок заново — снова install-apache-subpath.sh, он уже чинен.
#
# Переменные: VHOST=/путь/к/файлу.conf, SERVER_NAME=airflow-test.oms66.ru

set -uo pipefail

SERVER_NAME="${SERVER_NAME:-airflow-test.oms66.ru}"

ok()   { printf '  \033[32m✓\033[0m %s\n' "$1"; }
bad()  { printf '  \033[31m✗\033[0m %s\n' "$1"; exit 1; }
warn() { printf '  \033[33m!\033[0m %s\n' "$1"; }

[ "$(id -u)" -eq 0 ] || bad "нужен root: sudo bash $0"
command -v python3 >/dev/null || bad "нужен python3"

# ── найти файл ───────────────────────────────────────────────────────────────
# Конфиг СЛОМАН, поэтому apache2ctl -S сейчас не ответит — ищем по каталогам.
# Обязательно grep -R, а не -r: sites-enabled состоит из симлинков, а -r по
# ссылкам не идёт (именно на этом прошлая версия «не нашла» существующий сайт).
if [ -z "${VHOST:-}" ]; then
  mapfile -t found < <(grep -Rls "ServerName[[:space:]]\+$SERVER_NAME" \
                       /etc/apache2/sites-enabled/ /etc/apache2/sites-available/ \
                       /etc/apache2/conf-enabled/ /etc/httpd/conf.d/ 2>/dev/null |
                       xargs -r -n1 readlink -f | sort -u)
  case ${#found[@]} in
    1) VHOST="${found[0]}" ;;
    0) bad "не нашёл файл с ServerName $SERVER_NAME — укажите VHOST=..." ;;
    *) printf '     %s\n' "${found[@]}"; bad "файлов несколько — укажите VHOST=..." ;;
  esac
fi
[ -L "$VHOST" ] && VHOST=$(readlink -f "$VHOST")
[ -f "$VHOST" ] || bad "нет файла $VHOST"
ok "vhost: $VHOST"

BACKUP="$VHOST.before-fix-$(date +%Y%m%d-%H%M%S)"
cp "$VHOST" "$BACKUP"
ok "копия до починки: $BACKUP"

# ── вырезать блок ────────────────────────────────────────────────────────────
VHOST="$VHOST" python3 - <<'PY'
import os, re, sys

path = os.environ["VHOST"]
text = open(path, encoding="utf-8").read()
before = text

removed = 0
for name in ("etl_configurator", "etl_builder"):
    # Хвост после закрывающего маркера — это приклеенная строка. Возвращаем ей
    # перевод строки вместо того, чтобы удалить вместе с блоком: там может
    # лежать </VirtualHost> или ProxyPass, без которых конфиг бессмыслен.
    pattern = re.compile(
        r"[ \t]*# >>> " + name + r"\b.*?# <<< " + name + r"([^\n]*)\n?", re.S)

    def repl(m):
        tail = m.group(1)
        return (tail.lstrip() + "\n") if tail.strip() else ""

    text, n = pattern.subn(repl, text)
    removed += n

if not removed:
    print("    блока конструктора в файле нет — менять нечего")
    sys.exit(2)

# Парность секций — дешёвая проверка до записи.
for tag in ("VirtualHost", "Location", "Directory", "Proxy", "IfModule"):
    op = len(re.findall(r"^[ \t]*<%s[ >]" % tag, text, re.M))
    cl = len(re.findall(r"^[ \t]*</%s>" % tag, text, re.M))
    if op != cl:
        sys.exit(f"после вырезания не сходятся <{tag}>: открыто {op}, закрыто {cl} "
                 f"— записывать не стал, разберитесь руками")

open(path, "w", encoding="utf-8").write(text)
print(f"    вырезано блоков: {removed}, файл стал короче на "
      f"{len(before.splitlines()) - len(text.splitlines())} строк")
PY
rc=$?
if [ "$rc" -eq 2 ]; then
  rm -f "$BACKUP"
  ok "правки не потребовалось"
elif [ "$rc" -ne 0 ]; then
  cp "$BACKUP" "$VHOST"
  bad "починка не удалась — файл возвращён из копии"
else
  ok "блок конструктора вырезан"
fi

# ── проверка ─────────────────────────────────────────────────────────────────
echo
CTL=$(command -v apache2ctl || command -v apachectl)
if out=$("$CTL" configtest 2>&1); then
  ok "configtest прошёл: ${out##*$'\n'}"
  echo
  echo "Теперь можно перечитать конфиг и поставить блок заново:"
  echo "  sudo systemctl reload apache2"
  echo "  sudo bash deploy/dagbuilder/install-apache-subpath.sh"
else
  echo "$out" | sed 's/^/     /'
  warn "configtest всё ещё не проходит — значит сломано что-то ещё."
  warn "Копия до починки: $BACKUP"
fi
