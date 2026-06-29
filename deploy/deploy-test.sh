#!/usr/bin/env bash
#
# Коллега (на Jupyter): закоммитить правки в общем клоне и выкатить их в
# тестовый airflow одной командой.
#
#   bash deploy/deploy-test.sh "что сделал"
#
# Что делает:
#   1. add -A + commit с твоим сообщением (если есть что коммитить);
#   2. синхронизация с bare-репо (rebase своих коммитов поверх чужих) —
#      чтобы не разойтись с дагами других и push прошёл fast-forward;
#   3. проверка, что конфиг собирается (битый json / дубликат ключа линии);
#   4. push в bare-репо (origin) — post-receive сам разложит код в test-src
#      и перезапустит airflow-test.
#
# Ничего страшного при ошибке не произойдёт: на каждом шаге скрипт
# останавливается с понятным сообщением, ничего «наполовину» не выкатывая.
#
set -euo pipefail

BRANCH=${DEPLOY_BRANCH:-test}     # ветка деплоя
REMOTE=${REMOTE:-origin}          # в клоне на Jupyter origin = серверный bare-репо

cd "$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"

# сообщение опционально: для быстрых правок подставляется дата
msg=${1:-"deploy $(date '+%Y-%m-%d %H:%M')"}

cur=$(git rev-parse --abbrev-ref HEAD)
if [[ "$cur" != "$BRANCH" ]]; then
    echo "Ты на ветке '$cur', а деплоится '$BRANCH'. Переключись: git checkout $BRANCH"
    exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
    git add -A
    git commit -m "$msg"
else
    echo "Изменений нет — проверю синхронизацию и выкачу текущее состояние."
fi

echo "== синхронизация с $REMOTE/$BRANCH =="
git fetch "$REMOTE" "$BRANCH"
if ! git rebase "$REMOTE/$BRANCH"; then
    git rebase --abort || true
    echo "!! Конфликт: те же файлы правил кто-то ещё."
    echo "   Разреши вручную (git status) или позови Владислава. Ничего не выкачено."
    exit 1
fi

echo "== проверка сборки конфига =="
python3 tools/regen_config.py config SpTableName SpOnce

echo "== push -> $REMOTE/$BRANCH (запустит деплой и рестарт airflow-test) =="
git push "$REMOTE" "$BRANCH"

echo "Готово. Результат: https://airflow-test.oms66.ru"
