#!/usr/bin/env bash
#
# Выложить на ПРОД то, что отработало на тесте — целиком или по отдельным линиям.
#
#   bash deploy/deploy-prod.sh --diff                  посмотреть, чем прод отличается от теста
#   bash deploy/deploy-prod.sh --line prbsmoPostOrcl   выложить одну линию
#   bash deploy/deploy-prod.sh --line A --line B       несколько линий разом
#   bash deploy/deploy-prod.sh --paths <путь> [...]    выложить конкретные файлы
#   bash deploy/deploy-prod.sh                         выложить ВЕСЬ тест
#
#   FROM=<ref> — что считать «тестом» (по умолчанию origin/test)
#   YES=1      — без вопроса (для скриптов)
#
# Как это устроено. Прод — не «хвост» теста, а СОБРАННОЕ состояние: каждая
# выкладка это коммит поверх предыдущего прода, дерево которого взято с теста
# (целиком или только по нужным путям). Поэтому недоделанная работа на тесте
# на прод не уезжает: пока её не выложили явно, её там просто нет.
#
# История прода линейна и своя, push всегда fast-forward, откат — выкладка
# предыдущего тега prod-*.
#
# Точечная выкладка НЕ трогает общий код (Functions/, Src/, Connect/): такие
# правки едут только полной выкладкой, когда тест проверен целиком. Обойти
# можно осознанно — FORCE_CODE=1.
#
set -euo pipefail

BRANCH=${PROD_BRANCH:-prod}
REMOTE=${PROD_REMOTE:-prod}        # ssh://devel@airflow/opt/airflow-prod/etl.git
FROM=${FROM:-origin/test}          # что считаем «тестом»
CODE_RE='^(Functions|Src|Connect)/'

cd "$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
ROOT=$PWD

mode=full
lines=()
paths=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --line)  mode=partial; lines+=("$2"); shift 2 ;;
        --paths) mode=partial; shift; while [[ $# -gt 0 && $1 != --* ]]; do paths+=("$1"); shift; done ;;
        --diff)  mode=diff; shift ;;
        --help|-h) sed -n '2,32p' "$0" | sed 's/^# \?//'; exit 0 ;;
        *) echo "Не понял аргумент: $1 (см. --help)"; exit 1 ;;
    esac
done

git remote get-url "$REMOTE" >/dev/null 2>&1 || {
    echo "Нет remote '$REMOTE'. Добавь:"
    echo "  git remote add $REMOTE ssh://devel@airflow/opt/airflow-prod/etl.git"
    exit 1
}

echo "== синхронизация ссылок =="
git fetch --quiet origin 2>/dev/null || echo "  (origin недоступен, беру локальные ссылки)"
git fetch --quiet "$REMOTE" "$BRANCH" 2>/dev/null || true

NEW=$(git rev-parse --verify --quiet "$FROM^{commit}") || { echo "Не нашёл ref '$FROM'."; exit 1; }
BASE=$(git rev-parse --verify --quiet "$REMOTE/$BRANCH^{commit}" || true)

if [[ -z $BASE ]]; then
    echo "На проде ветки '$BRANCH' ещё нет — это первая выкатка, поедет весь тест."
    mode=full
fi

# ─────────────────────────── что отличается ───────────────────────────
if [[ $mode == diff ]]; then
    echo
    echo "== Прод:  $(git log -1 --format='%h %ad %s' --date=short "$BASE") =="
    echo "== Тест:  $(git log -1 --format='%h %ad %s' --date=short "$NEW") =="
    echo
    echo "Файлы, которые на тесте отличаются от прода:"
    git diff --stat "$BASE" "$NEW" | sed 's/^/  /'
    echo
    echo "Только общий код (Functions/Src/Connect):"
    git diff --name-only "$BASE" "$NEW" | grep -E "$CODE_RE" | sed 's/^/  /' || echo "  (нет отличий)"
    exit 0
fi

# ─────────────────────────── сборка выкладки ───────────────────────────
WT=$(mktemp -d /tmp/etl-prod-XXXXXX)
cleanup() { git worktree remove --force "$WT" >/dev/null 2>&1 || true; rm -rf "$WT"; }
trap cleanup EXIT

if [[ $mode == partial ]]; then
    # 1) в рабочем дереве теста выясняем полный список файлов линий
    git worktree add --detach --quiet "$WT" "$NEW"
    if [[ ${#lines[@]} -gt 0 ]]; then
        echo "== файлы линий: ${lines[*]} =="
        mapfile -t lineFiles < <(python3 "$ROOT/tools/line_files.py" --root "$WT" "${lines[@]}")
        paths+=("${lineFiles[@]}")
    fi
    [[ ${#paths[@]} -gt 0 ]] || { echo "Нечего выкладывать — не задано ни линий, ни путей."; exit 1; }

    blocked=$(printf '%s\n' "${paths[@]}" | grep -E "$CODE_RE" || true)
    if [[ -n $blocked && ${FORCE_CODE:-} != 1 ]]; then
        echo "!! Точечная выкладка задевает общий код:"
        printf '%s\n' "$blocked" | sed 's/^/     /'
        echo "   Такие правки едут полной выкладкой (bash deploy/deploy-prod.sh),"
        echo "   когда тест проверен целиком. Осознанно обойти: FORCE_CODE=1 ..."
        exit 1
    fi

    # 2) переводим дерево на текущее состояние прода и накладываем только эти пути
    git -C "$WT" checkout --detach --quiet "$BASE"
    for p in "${paths[@]}"; do
        if git cat-file -e "$NEW:$p" 2>/dev/null; then
            git -C "$WT" checkout "$NEW" -- "$p"
        else
            git -C "$WT" rm -r --quiet --ignore-unmatch -- "$p"   # на тесте файл удалён
        fi
    done
    title="частичная выкладка"
    [[ ${#lines[@]} -gt 0 ]] && title="линии: ${lines[*]}"
else
    # весь тест: дерево прода становится в точности деревом теста
    git worktree add --detach --quiet "$WT" "$BASE"
    git -C "$WT" read-tree -u --reset "$NEW"
    title="целиком: $FROM"
fi

if git -C "$WT" diff --cached --quiet && git -C "$WT" diff --quiet; then
    echo "Прод уже содержит это состояние — выкладывать нечего."
    exit 0
fi

echo
echo "== что уедет на прод ($title) =="
git -C "$WT" add -A
git -C "$WT" diff --cached --stat "$BASE" | sed 's/^/  /'

echo
echo "== проверка сборки конфига =="
python3 "$WT/tools/regen_config.py" config SpTableName SpOnce

if [[ ${YES:-} != 1 ]]; then
    echo
    read -r -p "Выкладываем на ПРОД? (y/N) " answer
    [[ $answer == "y" || $answer == "Y" ]] || { echo "Отменено."; exit 1; }
fi

msg="Выкладка на прод: $title
Источник: $FROM $(git log -1 --format=%h "$NEW")"
git -C "$WT" -c user.name="${GIT_AUTHOR_NAME:-$(git config user.name || echo deploy)}" \
    -c user.email="${GIT_AUTHOR_EMAIL:-$(git config user.email || echo deploy@local)}" \
    commit --quiet -m "$msg"
COMMIT=$(git -C "$WT" rev-parse HEAD)

# Тег с точностью до минуты читается человеком, но две выкладки подряд в одну
# минуту не редкость (правка -> сразу откат), поэтому добиваем суффиксом.
TAG="prod-$(date '+%Y%m%d-%H%M')"
suffix=2
while git rev-parse --verify --quiet "refs/tags/$TAG" >/dev/null; do
    TAG="prod-$(date '+%Y%m%d-%H%M')-$suffix"
    suffix=$((suffix + 1))
done
git branch -f "$BRANCH" "$COMMIT"
git tag -a "$TAG" "$COMMIT" -m "$title"

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
git push origin "$TAG" 2>/dev/null || echo "  (тег в origin не ушёл — не критично)"

PREV_TAG=$(git tag --list 'prod-*' --sort=-creatordate | sed -n 2p)
echo
echo "Готово: $TAG"
echo "  чем прод отличается от теста:  bash deploy/deploy-prod.sh --diff"
echo "  вернуть прошлую выкладку:     FROM=${PREV_TAG:-<прошлый тег prod-*>} bash deploy/deploy-prod.sh"
echo "                                (вернёт ВСЁ дерево того тега — новым коммитом, без force)"
echo "  полный откат на старый airflow: sudo bash setup-airflow-prod.sh --rollback"
