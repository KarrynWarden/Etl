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

if ! git rebase "$SERVER/$BRANCH"; then
    git rebase --abort || true
    echo "!! Конфликт с $SERVER/$BRANCH — правились одни и те же файлы. Разреши вручную."
    exit 1
fi

echo "Готово. Ветка '$BRANCH' содержит изменения коллег + твои сверху."
echo "Дальше, когда захочешь разослать: bash local/dev-push.sh"
