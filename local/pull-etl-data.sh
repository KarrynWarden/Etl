#!/usr/bin/env bash
#
# Запускать НА СЕРВЕРЕ (ssh devel@airflow, затем sudo -s).
#
# Собирает structures и queries/customQueries из прод-папок в один архив,
# чтобы перенести их в локальный etlFolder на dev-PC (для запуска линий локально).
#
# Источник по умолчанию — realAirflow; берём и etlFolderProd, и etlFolder
# (mocheck-структуры лежат в etlFolder, остальные — в etlFolderProd), сливая в
# одну папку structures/ и queries/customQueries/.
#
set -euo pipefail

BASE="${1:-/opt/jupyter/workdir/konkin/realAirflow}"
STAGE="$(mktemp -d)"
mkdir -p "$STAGE/structures" "$STAGE/queries/customQueries"

found=0
for ef in etlFolderProd etlFolder; do
    src="$BASE/$ef"
    if [[ -d "$src/structures" ]]; then
        cp -a "$src/structures/." "$STAGE/structures/" && found=1
        echo "  + $src/structures"
    fi
    if [[ -d "$src/queries/customQueries" ]]; then
        cp -a "$src/queries/customQueries/." "$STAGE/queries/customQueries/" && found=1
        echo "  + $src/queries/customQueries"
    fi
done

if [[ "$found" -eq 0 ]]; then
    echo "ОШИБКА: в $BASE не найдено ни structures, ни queries/customQueries." >&2
    echo "Передай правильный путь аргументом: bash pull-etl-data.sh /путь/к/realAirflow" >&2
    rm -rf "$STAGE"
    exit 1
fi

OUT="/home/devel/etl-data-$(date +%Y%m%d).tar.gz"
tar czf "$OUT" -C "$STAGE" structures queries
chown devel:devel "$OUT" 2>/dev/null || true
rm -rf "$STAGE"

echo "Готово: $OUT  ($(du -h "$OUT" | cut -f1))"
echo
echo "Теперь на dev-PC (сольёт structures/ и queries/customQueries/ в локальный etlFolder):"
echo "  scp devel@airflow:$OUT ~/"
echo "  tar xzf ~/$(basename "$OUT") -C ~/konkin/etl/etlFolder"
