#!/usr/bin/env bash
#
# dev-PC (Владислав): разослать ветку test в gitea (канон) и на сервер (деплой).
#
#   bash local/dev-push.sh
#
# Перед пушем синхронизируется с сервером (rebase), чтобы не обогнать коллег и
# чтобы push прошёл fast-forward. Сетевые push'и повторяются с backoff.
#
# Remote'ы по умолчанию:
#   origin -> gitea (https://gitea.oms66.ru/Konkin/ETL.git)   [канон]
#   server -> ssh://devel@airflow/opt/airflow-test/etl.git    [деплой]
#
set -euo pipefail

BRANCH=${DEPLOY_BRANCH:-test}
GITEA=${GITEA_REMOTE:-origin}
SERVER=${SERVER_REMOTE:-server}

cd "$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"

# push с повтором при сетевых сбоях: 2s, 4s, 8s, 16s
push_retry() {
    local remote=$1 branch=$2 delay=2
    for attempt in 1 2 3 4 5; do
        if git push "$remote" "$branch"; then
            return 0
        fi
        if [[ $attempt -eq 5 ]]; then
            echo "!! push в $remote не удался после повторов."
            return 1
        fi
        echo "   push в $remote не прошёл, повтор через ${delay}s..."
        sleep "$delay"
        delay=$((delay * 2))
    done
}

cur=$(git rev-parse --abbrev-ref HEAD)
if [[ "$cur" != "$BRANCH" ]]; then
    echo "Ты на ветке '$cur', а нужна '$BRANCH'. Переключись: git checkout $BRANCH"
    exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
    echo "!! Есть несохранённые правки — закоммить их сначала."
    exit 1
fi

echo "== синхронизация с $SERVER/$BRANCH перед пушем =="
git fetch "$SERVER" "$BRANCH"
if ! git rebase "$SERVER/$BRANCH"; then
    git rebase --abort || true
    echo "!! Конфликт с $SERVER/$BRANCH. Сначала: bash local/dev-pull.sh и разреши."
    exit 1
fi

echo "== push -> $GITEA/$BRANCH (gitea, канон) =="
push_retry "$GITEA" "$BRANCH"

echo "== push -> $SERVER/$BRANCH (сервер -> деплой airflow-test) =="
push_retry "$SERVER" "$BRANCH"

echo "Готово: gitea и сервер синхронизированы, тестовый airflow перезапущен."
