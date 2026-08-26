#!/usr/bin/env bash
#
# Перезапустились ли сервисы после последнего push'а в test.
#
#   bash deploy/check-restart.sh
#
# Зачем отдельный скрипт. Перезапуск после push'а бывает ДВУХ видов, и по
# выводу хука их не различить на глаз:
#
#   «перезапущено: …»          — сделал сам хук через sudo (push пришёл по ssh);
#   «заявка на перезапуск …»   — sudo из процесса недоступен (push из
#                                конструктора: NoNewPrivileges), рестарт делает
#                                path-юнит СЕКУНДЫ СПУСТЯ и уже вне push'а.
#
# Во втором случае хук про исход ничего сказать не может: перезапуск происходит
# после того, как он закончился, — а конструктор при этом перезапускает сам
# себя, то есть убивает процесс, в котором хук и работал. Единственный честный
# способ проверить — посмотреть, когда сервисы стартовали на самом деле.
#
# Прав не требует: `systemctl show` читают все.
set -uo pipefail

STAMP=${ETL_RESTART_STAMP:-/opt/airflow-test/.restart-request}
UNITS=(airflow-test-scheduler airflow-test-webserver etl-dagbuilder-api)
SRC=${ETL_TEST_SRC:-/opt/airflow-test/test-src}

echo "== последний коммит в разложенном дереве =="
if [[ -d "$SRC/.git" ]] || git -C "$SRC" rev-parse --git-dir >/dev/null 2>&1; then
    git -C "$SRC" log -1 --format='  %h  %ad  %s' --date=format:'%d.%m %H:%M:%S' 2>/dev/null \
        || echo "  (не прочитать — чужой владелец?)"
else
    echo "  ($SRC не git-дерево)"
fi

echo
echo "== заявка на перезапуск =="
if [[ -e "$STAMP" ]]; then
    printf '  %s  изменён %s\n' "$STAMP" \
        "$(date -r "$STAMP" '+%d.%m %H:%M:%S' 2>/dev/null || echo '?')"
else
    echo "  $STAMP не существует — заявок ещё не было"
fi

echo
echo "== когда сервисы стартовали =="
# ActiveEnterTimestamp — момент, когда юнит стал active. Если он ПОЗЖЕ коммита
# и позже заявки, перезапуск состоялся; если раньше — сервис работает на старом
# коде, что бы ни писал вывод push'а.
for u in "${UNITS[@]}"; do
    if ! systemctl list-unit-files "$u.service" >/dev/null 2>&1; then
        printf '  %-28s (юнита нет)\n' "$u"
        continue
    fi
    state=$(systemctl is-active "$u" 2>/dev/null)
    started=$(systemctl show -p ActiveEnterTimestamp --value "$u" 2>/dev/null)
    printf '  %-28s %-10s %s\n' "$u" "$state" "${started:-—}"
done

echo
echo "== path-юнит, исполняющий заявки =="
if systemctl list-unit-files etl-deploy-restart.path >/dev/null 2>&1; then
    printf '  %-28s %s\n' "etl-deploy-restart.path" "$(systemctl is-active etl-deploy-restart.path)"
    echo "  последние срабатывания:"
    journalctl -u etl-deploy-restart.service -n 5 --no-pager -o short 2>/dev/null \
        | sed 's/^/    /' || echo "    (журнал недоступен под $(id -un))"
else
    echo "  НЕ УСТАНОВЛЕН — push из конструктора сервисы не перезапустит."
    echo "  Поставить: sudo bash deploy/setup-airflow-test.sh (шаг 9d)"
fi

echo
echo "Читать так: время старта сервиса должно быть ПОЗЖЕ времени коммита."
echo "Раньше — значит сервис работает на старом коде, и вывод push'а тут не"
echo "указ: заявку он подаёт, а исполняется она уже без него."
