#!/usr/bin/env bash
#
# dev-PC (Владислав): подтянуть к себе изменения коллег с серверного bare-репо,
# НЕ затирая свою работу — свои локальные коммиты ложатся поверх (rebase).
#
#   bash local/dev-pull.sh
#
# Remote 'server' должен указывать на серверный bare-репо, например:
#   git remote add server ssh://devel@airflow/opt/airflow-test/etl.git
#
set -euo pipefail

BRANCH=${DEPLOY_BRANCH:-test}
SERVER=${SERVER_REMOTE:-server}

cd "$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"

cur=$(git rev-parse --abbrev-ref HEAD)
if [[ "$cur" != "$BRANCH" ]]; then
    echo "Ты на ветке '$cur', а нужна '$BRANCH'. Переключись: git checkout $BRANCH"
    exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
    echo "!! Есть несохранённые правки. Сначала закоммить их или спрячь: git stash"
    exit 1
fi

echo "== fetch $SERVER/$BRANCH =="
git fetch "$SERVER" "$BRANCH"

# Конфликт НЕ откатываем — в этом весь смысл dev-pull.sh: он для того и есть,
# чтобы конфликт разрешить. Раньше здесь стоял `git rebase --abort`, и скрипт
# просил «разреши вручную», выбросив ровно то состояние, в котором это можно
# сделать: следом `git rebase --continue` отвечал «Нет перемещения в процессе»,
# а `git add` — «спецификатор пути не соответствует ни одному файлу».
if ! git rebase "$SERVER/$BRANCH"; then
    echo
    echo "!! Конфликт с $SERVER/$BRANCH — правились одни и те же файлы."
    echo "   Перемещение ОСТАВЛЕНО в процессе, разбирай прямо сейчас:"
    echo
    git --no-pager diff --name-only --diff-filter=U | sed 's/^/     /'
    echo
    echo "   Для каждого файла из списка (стороны в rebase ПЕРЕВЁРНУТЫ):"
    echo "     git show :2:<файл>   # версия с сервера ($SERVER/$BRANCH)"
    echo "     git show :3:<файл>   # твой коммит, который накладывается"
    echo "   Правишь файл, убираешь маркеры, затем:"
    echo "     git add <файл> && git rebase --continue"
    echo
    echo "   Передумал — вернуть всё как было: git rebase --abort"
    exit 1
fi

echo "Готово. Ветка '$BRANCH' содержит изменения коллег + твои сверху."
echo "Дальше, когда захочешь разослать: bash local/dev-push.sh"
