#!/usr/bin/env bash
#
# Выкатить на ПРОД то, что уже отработало на тесте.
#
#   bash deploy/deploy-prod.sh              # прод <- origin/test (по умолчанию)
#   bash deploy/deploy-prod.sh <ref>        # прод <- конкретный коммит/тег
#   YES=1 bash deploy/deploy-prod.sh        # без вопроса (для скриптов)
#
# Что делает:
#   1. проверяет, что рабочая копия чистая и remote прода настроен;
#   2. показывает, ЧТО именно уедет (список коммитов и файлов) и спрашивает y/n;
#   3. проверяет сборку конфига (битый json / дубликат ключа линии);
#   4. переводит локальную ветку prod на выбранный ref БЕЗ merge-коммита
#      (fast-forward или прямая установка) и ставит тег prod-ГГГГММДД-ЧЧММ —
#      по нему делается откат кода;
#   5. пушит ветку и тег в bare-репо прода; post-receive раскладывает код,
#      проверяет парсинг дагов и (если прод уже переключён) перезапускает его.
#
# Прод НЕ переключается этим скриптом — переключение делается один раз на
# сервере (setup-airflow-prod.sh --cutover), см. deploy/README-prod.md.
#
set -euo pipefail

BRANCH=${PROD_BRANCH:-prod}
REMOTE=${PROD_REMOTE:-prod}        # ssh://devel@airflow/opt/airflow-prod/etl.git
SOURCE_REF=${1:-origin/test}       # что именно выкатываем

cd "$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"

if ! git remote get-url "$REMOTE" >/dev/null 2>&1; then
    echo "Нет remote '$REMOTE'. Добавь:"
    echo "  git remote add $REMOTE ssh://devel@airflow/opt/airflow-prod/etl.git"
    exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
    echo "Рабочая копия не чистая — на прод выкатывается только зафиксированное."
    git status --short
    exit 1
fi

echo "== что выкатываем =="
git fetch --quiet origin || echo "  (origin недоступен, беру локальные ссылки)"
git fetch --quiet "$REMOTE" "$BRANCH" 2>/dev/null || true

if ! NEW=$(git rev-parse --verify --quiet "$SOURCE_REF^{commit}"); then
    echo "Не нашёл ref '$SOURCE_REF'."
    exit 1
fi
CUR=$(git rev-parse --verify --quiet "$REMOTE/$BRANCH^{commit}" || true)

if [[ -z "$CUR" ]]; then
    echo "На проде ветки '$BRANCH' ещё нет — это первая выкатка."
    echo "Коммит: $(git log -1 --oneline "$NEW")"
else
    if [[ "$CUR" == "$NEW" ]]; then
        echo "Прод уже на $(git log -1 --oneline "$NEW") — выкатывать нечего."
        exit 0
    fi
    if ! git merge-base --is-ancestor "$CUR" "$NEW"; then
        echo "!! $SOURCE_REF НЕ является продолжением того, что на проде."
        echo "   Разойтись историей с продом нельзя — разберись вручную:"
        echo "     git log --oneline $REMOTE/$BRANCH ^$SOURCE_REF"
        exit 1
    fi
    echo "Коммиты, которые уедут на прод:"
    git log --oneline "$CUR..$NEW" | sed 's/^/  /'
    echo
    echo "Файлы:"
    git diff --stat "$CUR" "$NEW" | sed 's/^/  /'
fi

echo
echo "== проверка сборки конфига =="
python3 tools/regen_config.py config SpTableName SpOnce

if [[ "${YES:-}" != "1" ]]; then
    echo
    read -r -p "Выкатываем на ПРОД? (y/N) " answer
    [[ "$answer" == "y" || "$answer" == "Y" ]] || { echo "Отменено."; exit 1; }
fi

TAG="prod-$(date '+%Y%m%d-%H%M')"
echo "== ветка $BRANCH -> $NEW, тег $TAG =="
git branch -f "$BRANCH" "$NEW"
git tag -a "$TAG" "$NEW" -m "Выкатка на прод $(date '+%Y-%m-%d %H:%M')"

push_retry() {   # сеть иногда отваливается: 2s, 4s, 8s, 16s
    local delay=2
    for attempt in 1 2 3 4 5; do
        if git push "$@"; then return 0; fi
        if [[ $attempt -eq 5 ]]; then echo "!! push не удался после повторов."; return 1; fi
        echo "   не прошло, повтор через ${delay}s..."
        sleep "$delay"; delay=$((delay * 2))
    done
}

echo "== push -> $REMOTE/$BRANCH =="
push_retry "$REMOTE" "$BRANCH"
push_retry "$REMOTE" "$TAG"
# Тот же тег в канон (gitea), чтобы история выкаток была видна и там.
git push origin "$TAG" 2>/dev/null || echo "  (тег в origin не ушёл — не критично)"

echo
echo "Готово. Что дальше:"
echo "  - если прод ещё не переключён: sudo bash setup-airflow-prod.sh --cutover на сервере;"
echo "  - откат кода: bash deploy/deploy-prod.sh <предыдущий тег prod-*>"
echo "    (тег старше текущего прода — push отобьётся, тогда"
echo "     git push $REMOTE +$BRANCH явно и осознанно);"
echo "  - полный откат прода на старую версию: sudo bash setup-airflow-prod.sh --rollback"
