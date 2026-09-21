#!/usr/bin/env bash
#
# Общий разбор сетевых отказов при обращении к remote 'server' по ssh.
# Подключается через `source` из dev-push.sh и dev-pull.sh.
#
# Зачем отдельным файлом: оба скрипта первым делом делают `git fetch server`, и
# при set -e падение на этой строке выглядит как голый вывод ssh. Для ошибок
# уровня рукопожатия этого мало: git сообщает «Не удалось прочитать из внешнего
# репозитория» и предлагает проверить права доступа, хотя до проверки прав дело
# вообще не дошло и чинить надо не права.

# git fetch, а при неудаче — объяснение вместо голого вывода ssh.
fetch_or_explain() {
    local remote=$1 branch=$2 log rc
    log=$(mktemp) || return 1
    if { git fetch "$remote" "$branch" 2>&1; echo $? >"$log.rc"; } | tee "$log"; then :; fi
    rc=$(cat "$log.rc" 2>/dev/null || echo 1)
    if [[ "$rc" == "0" ]]; then rm -f "$log" "$log.rc"; return 0; fi
    explain_ssh_failure "$remote" "$log"
    rm -f "$log" "$log.rc"
    return 1
}

explain_ssh_failure() {
    local remote=$1 log=$2 url
    url=$(git remote get-url "$remote" 2>/dev/null || echo '?')
    echo
    echo "!! Не достучались до '$remote' ($url)."

    # Рукопожатие ssh: ключ хоста. Сервер предлагает ssh-rsa (RSA с SHA-1), а
    # OpenSSH с версии 8.8 такие ключи хоста по умолчанию не принимает. Это
    # ломается НЕ в день обновления сервера, а в день, когда на рабочем ПК
    # обновился ssh или пропал ~/.ssh/config с разрешением. Ни к правам, ни к
    # репозиторию отношения не имеет: до проверки ключа пользователя тут ещё
    # не дошло.
    if grep -q 'no matching host key type' "$log"; then
        local host
        host=$(ssh_host_from_url "$url")
        echo "   Причина: ssh отказался от ключа ХОСТА. Сервер предлагает ssh-rsa,"
        echo "   а OpenSSH с 8.8 такие ключи по умолчанию не принимает."
        echo "   Твой ключ и права тут ни при чём — до них дело не дошло."
        echo
        echo "   Разрешить для этого хоста — в ~/.ssh/config:"
        echo
        echo "     Host ${host:-airflow}"
        echo "         HostkeyAlgorithms +ssh-rsa"
        echo "         PubkeyAcceptedAlgorithms +ssh-rsa"
        echo
        echo "   Если ssh старее 8.5, вторая строка называется PubkeyAcceptedKeyTypes"
        echo "   (ssh -V покажет версию; неизвестное слово в config — фатальная ошибка,"
        echo "   поэтому пиши ровно одно из двух)."
        echo "   Разово, не трогая config:"
        echo "     git -c core.sshCommand='ssh -o HostkeyAlgorithms=+ssh-rsa' fetch $remote"
        return 0
    fi

    if grep -qi 'host key verification failed' "$log"; then
        echo "   Причина: ключ хоста не совпал с записанным в ~/.ssh/known_hosts."
        echo "   Либо сервер переставили, либо файл потёрли. Убедись, что это тот сервер,"
        echo "   и только потом удаляй старую запись:"
        echo "     ssh-keygen -R $(ssh_host_from_url "$url")"
        return 0
    fi

    if grep -qi 'permission denied' "$log"; then
        echo "   Причина: сервер не принял ключ ПОЛЬЗОВАТЕЛЯ. Рукопожатие прошло."
        echo "   Проверь, что ключ на месте и предъявляется:"
        echo "     ls -l ~/.ssh/id_* && ssh -v $(ssh_host_from_url "$url") true"
        return 0
    fi

    echo "   Что смотреть: ssh -v $(ssh_host_from_url "$url") true"
}

# host (без пользователя и пути) из url remote'а — для подсказок и ssh-keygen -R
ssh_host_from_url() {
    local url=$1 dest=
    case "$url" in
        ssh://*) url=${url#ssh://}; dest=${url%%/*} ;;
        *:*)     dest=${url%%:*} ;;
        *)       printf '%s\n' '<сервер>'; return 0 ;;
    esac
    dest=${dest#*@}          # убрать user@
    printf '%s\n' "${dest%%:*}"   # убрать :port
}
