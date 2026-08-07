#!/usr/bin/env bash
#
# dev-PC (Владислав): разослать ветку test в gitea (канон) и на сервер (деплой).
#
#   bash local/dev-push.sh
#
# Перед пушем синхронизируется с сервером (rebase), чтобы не обогнать коллег и
# чтобы push прошёл fast-forward. Сетевые push'и повторяются с backoff.
#
# После пуша проверяет общий Jupyter-клон на сервере: если в его рабочей копии
# остались несохранённые правки (из-за них хук не смог обновить клон), скрипт
# показывает, что изменено, и спрашивает y/n на принудительную синхронизацию.
# Правки в файлах процессов (dags/structures/customQueries) подсвечиваются
# отдельно с diff'ом — их могли сделать коллеги.
#
# Remote'ы по умолчанию:
#   origin -> gitea (https://gitea.oms66.ru/Konkin/ETL.git)   [канон]
#   server -> ssh://devel@airflow/opt/airflow-test/etl.git    [деплой]
#
set -euo pipefail

BRANCH=${DEPLOY_BRANCH:-test}
GITEA=${GITEA_REMOTE:-origin}
SERVER=${SERVER_REMOTE:-server}
# Общий редактируемый клон на сервере (там же Jupyter). Хук обновляет его ff-only,
# поэтому несохранённые правки в нём блокируют апдейт — это разбираем в конце.
JUPYTER_CLONE=${JUPYTER_CLONE:-/opt/jupyter/workdir/konkin/etl}
# «Файлы процессов»: правки тут могли сделать коллеги — их стоит изучить, а не
# затирать вслепую. Остальное (README, tools и т.п.) — локальная версия новее.
# Сюда же линии справочников/разового переноса: SpTableName.d/, SpOnce.d/ и
# queries/sp/ — это тоже файлы процессов, собранные конструктором. Без них
# новые sp-линии попадали в раздел «прочее, локальная версия новее» и скрипт
# предлагал их затереть, хотя это свежая работа, а не устаревшая правка.
PROTECTED_RE='(^|/)dags/|(^|/)structures/|(^|/)customQueries/|(^|/)config\.d/|(^|/)SpTableName\.d/|(^|/)SpOnce\.d/|(^|/)queries/sp/'

cd "$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"

# push с повтором при СЕТЕВЫХ сбоях: 2s, 4s, 8s, 16s.
# Отказ сервера повторять бессмысленно: pre-receive через две секунды ответит
# ровно то же самое, а человек в это время смотрит на четыре одинаковых портянки
# с ошибкой и ждёт полминуты. Поэтому вывод ловим и по нему отличаем «сервер
# сказал нет» от «связь оборвалась».
REJECT_RE='pre-receive hook declined|remote rejected|\[rejected\]|non-fast-forward|protected branch|hook declined'
push_retry() {
    local remote=$1 branch=$2 delay=2 out rc
    for attempt in 1 2 3 4 5; do
        out=$(git push "$remote" "$branch" 2>&1); rc=$?
        printf '%s\n' "$out"
        [[ $rc -eq 0 ]] && return 0
        if grep -qE "$REJECT_RE" <<<"$out"; then
            echo "!! Сервер ОТКЛОНИЛ push — это не сетевой сбой, повторять нечего."
            return 1
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

# ssh-адрес сервера (user@host) из URL remote'а server
ssh_dest_from_remote() {
    local url
    url=$(git remote get-url "$SERVER" 2>/dev/null) || return 1
    case "$url" in
        ssh://*) url=${url#ssh://}; printf '%s\n' "${url%%/*}" ;;  # ssh://user@host/path
        *:*)     printf '%s\n' "${url%%:*}" ;;                     # user@host:path
        *)       return 1 ;;
    esac
}

# Проверить рабочую копию Jupyter-клона на сервере и, если она «грязная»
# (хук не смог обновить её ff-only), предложить принудительную синхронизацию.
# Файлы процессов (dags/structures/customQueries) могли править коллеги — для них
# показываем отличия; всё прочее почти наверняка устаревшая правка на сервере.
sync_jupyter_clone() {
    local dest
    dest=$(ssh_dest_from_remote) || {
        echo "(не разобрал ssh-адрес '$SERVER' — пропускаю проверку Jupyter-клона)"; return 0; }

    echo "== проверка Jupyter-клона ($dest:$JUPYTER_CLONE) =="
    local status
    if ! status=$(ssh "$dest" "git -C '$JUPYTER_CLONE' status --porcelain" 2>/dev/null); then
        echo "   не удалось проверить Jupyter-клон по ssh — пропуск."
        return 0
    fi
    if [[ -z "$status" ]]; then
        echo "   Jupyter-клон чист — хук уже обновил его до '$BRANCH'."
        return 0
    fi

    local files protected other
    files=$(cut -c4- <<<"$status")                       # пути без двухсимвольного статуса
    protected=$(grep -E "$PROTECTED_RE" <<<"$files" || true)
    other=$(grep -Ev "$PROTECTED_RE" <<<"$files" || true)

    echo "!! В Jupyter-клоне есть несохранённые правки — новая версия туда НЕ доехала:"
    if [[ -n "$protected" ]]; then
        echo
        echo "  ⚠ Файлы процессов (dags / structures / customQueries) — это МОГЛИ быть коллеги:"
        sed 's/^/      /' <<<"$protected"
        echo "   --- их отличия от ветки '$BRANCH' (на сервере) ---"
        # shellcheck disable=SC2046
        ssh "$dest" "cd '$JUPYTER_CLONE' && git --no-pager diff -- $(tr '\n' ' ' <<<"$protected")" \
            2>/dev/null | sed 's/^/      /' || true
        echo "   ---------------------------------------------------"
    fi
    if [[ -n "$other" ]]; then
        echo
        echo "  Прочие файлы (вне процессов) — почти наверняка устаревшие правки на сервере,"
        echo "  локальная версия новее:"
        sed 's/^/      /' <<<"$other"
    fi

    echo
    [[ -n "$protected" ]] && \
        echo "ВНИМАНИЕ: затронуты файлы процессов — перезапись СОТРЁТ их на сервере!"
    local ans=""
    read -r -p "Перезаписать рабочую копию на сервере версией из '$BRANCH'? (y/N) " ans || ans=""
    if [[ "$ans" == "y" || "$ans" == "Y" ]]; then
        # umask 002 — чтобы переписанные файлы остались груп-записываемыми (etldev).
        if ssh "$dest" "umask 002; git -C '$JUPYTER_CLONE' fetch -q origin '$BRANCH' \
            && git -C '$JUPYTER_CLONE' reset --hard 'origin/$BRANCH' \
            && git -C '$JUPYTER_CLONE' clean -fd"; then
            echo "   Jupyter-клон принудительно синхронизирован с '$BRANCH'."
        else
            echo "!! Не удалось перезаписать Jupyter-клон — похоже, права на каталоги"
            echo "   (для удаления файла нужна запись на его папку, а не на сам файл)."
            echo "   Один раз выполни на сервере от root, чтобы клон стал общим для группы etldev:"
            echo "     sudo chgrp -R etldev $JUPYTER_CLONE"
            echo "     sudo find $JUPYTER_CLONE -type d -exec chmod 2775 {} +"
            echo "   затем снова запусти bash local/dev-push.sh и ответь y."
        fi
    else
        echo "   Оставил как есть. Разобраться вручную на сервере:"
        echo "     ssh $dest"
        echo "     git -C $JUPYTER_CLONE status      # затем git diff / git stash / git checkout -- <файл>"
    fi
}

# сообщение опционально: для быстрых правок подставляется дата
msg=${1:-"update $(date '+%Y-%m-%d %H:%M')"}

cur=$(git rev-parse --abbrev-ref HEAD)
if [[ "$cur" != "$BRANCH" ]]; then
    echo "Ты на ветке '$cur', а нужна '$BRANCH'. Переключись: git checkout $BRANCH"
    exit 1
fi

# есть несохранённые правки — закоммитить их сами (add -A + commit)
if [[ -n "$(git status --porcelain)" ]]; then
    git add -A
    git commit -m "$msg"
fi

echo "== синхронизация с $SERVER/$BRANCH перед пушем =="
git fetch "$SERVER" "$BRANCH"
if ! git rebase "$SERVER/$BRANCH"; then
    git rebase --abort || true
    echo "!! Конфликт с $SERVER/$BRANCH. Сначала: bash local/dev-pull.sh и разреши."
    exit 1
fi

# Гейт ДО пушей: синтаксис, сборка конфига, парсинг DAG'ов (см. tools/check_dags.py).
# Отдельной командой это не сделано намеренно: проверка, которую надо не забыть
# запустить, не запускается. Сервер поднимать не нужно — DagBag разбирает файлы
# как библиотека. Обойти осознанно: SKIP_CHECKS=1 bash local/dev-push.sh
if [[ "${SKIP_CHECKS:-}" == "1" ]]; then
    echo "!! ПРОВЕРКА ПРОПУЩЕНА (SKIP_CHECKS=1) — на свой страх."
else
    set +e
    bash "$(git rev-parse --show-toplevel)/deploy/check-dags.sh"
    checkRc=$?
    set -e
    case $checkRc in
        0) ;;
        2) echo
           echo "!! Даги НЕ парсились — нет пакета airflow в доступных интерпретаторах."
           echo "   Синтаксис и конфиг чисты, но ошибку уровня импорта здесь не поймать."
           echo "   Поправить: ETL_CHECK_PYTHON=/путь/к/python-с-airflow (см. local/README.md)."
           read -r -p "   Пушить всё равно? (y/N) " ans || ans=""
           [[ "$ans" == "y" || "$ans" == "Y" ]] || { echo "Отменено."; exit 1; } ;;
        *) echo
           echo "!! Проверка не прошла — push отменён. Почини и запусти снова."
           exit 1 ;;
    esac
fi

echo "== push -> $GITEA/$BRANCH (gitea, канон) =="
push_retry "$GITEA" "$BRANCH"

echo "== push -> $SERVER/$BRANCH (сервер -> деплой airflow-test) =="
push_retry "$SERVER" "$BRANCH"

# Jupyter-клон на сервере хук обновляет ff-only; если там есть несохранённые
# правки — разбираем их интерактивно (показать отличия / перезаписать).
sync_jupyter_clone

echo "Готово: gitea и сервер синхронизированы, тестовый airflow перезапущен."
