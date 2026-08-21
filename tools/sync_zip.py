#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Перенос правок с GitHub на рабочий ПК через zip — слиянием, а не копированием.

    python3 tools/sync_zip.py /media/flash/Etl-main.zip            отчёт + слияние
    python3 tools/sync_zip.py /media/flash/Etl-main.zip --dry-run   только отчёт
    python3 tools/sync_zip.py /media/flash/Etl-main.zip --base      первый раз
    python3 tools/sync_zip.py /media/flash/Etl-main.zip --yes       без вопроса

Windows (PowerShell) — там команда `python`:

    cd C:\путь\к\Etl
    python tools\sync_zip.py D:\Etl-main.zip --base

ЗАПУСКАТЬ ИЗ ПАПКИ ПРОЕКТА (рабочая копия ищется от текущего каталога) и
ТРЕТЬИМ ПИТОНОМ. На рабочей машине `python` — второй, и раньше он падал с
«SyntaxError: invalid syntax» на первой же строке; теперь файл разбирается и
вторым питоном тоже, а обнаружив себя под ним, перезапускается третьим сам.

Путь к архиву — точка МОНТИРОВАНИЯ, а не устройство: `/dev/sdc1` это
устройство, файлов по такому пути не бывает. Где смонтировано, показывает
`lsblk -o NAME,LABEL,MOUNTPOINT` (обычно /media/<юзер>/<метка>). Ошиблись —
скрипт поищет *.zip на подключённых носителях и назовёт найденное.

ЗАЧЕМ. Между двумя ПК только флешка, а с GitHub скачивается ровно одно —
архив всего дерева. Поэтому правки переносились копированием папок с заменой, и
у этого способа есть свойство, которое годами копит беду: копирование ДОБАВЛЯЕТ
и ОБНОВЛЯЕТ, но никогда не удаляет и не переименовывает. Так на тесте оказались
пятнадцать старых сборок фронтенда, SqlArea.jsx рядом с пришедшим ему на смену
CodeArea.jsx и примеры конфигов под тремя именами сразу.

Мусор в tools/ безобиден. Опасно то же самое в etlFolder/: удалённая линия не
удалится — её фрагмент останется лежать и вернётся в работу молча, потому что
для дага это просто ещё одна запись конфига.

КАК ЭТО РЕШАЕТСЯ. Архив — это СНИМОК дерева, а git умеет сливать снимки, если
объяснить ему, где чей. Скрипт заводит ветку-снимок (github-snapshot): каждый
принесённый архив становится на ней коммитом. Дальше обычное трёхстороннее
слияние, где

    база   = прошлый принесённый архив  (что я знал в прошлый раз)
    их     = нынешний архив             (что пришло сейчас)
    наше   = ваша ветка                 (что вы делали у себя)

и git сам разбирается: файл, которого в новом архиве нет, а в прошлом был, —
УДАЛЯЕТСЯ; файл, которого нет ни в одном архиве, — ваш, его не трогают вовсе;
файл, который поменялся с обеих сторон, — КОНФЛИКТ, и вы смотрите его глазами.
Ровно то, что нужно: и мусор уходит, и ваши запросы со структурами целы.

Заодно исчезает вопрос «а перенёс ли я тот коммит, где менялся etlFolder».
Сливаются не коммиты, а снимки: сколько бы коммитов ни было между архивами и в
каком бы порядке они ни шли, база — это всегда «прошлый архив».

ПЕРВЫЙ ЗАПУСК (--base). Базы ещё нет, и у ветки-снимка нет общей истории с
вашей — git такое сливать отказывается, и правильно делает. Поэтому первый
архив ПРИШИВАЕТСЯ слиянием `-s ours`: оно записывает снимок в предки вашей
ветки, НЕ МЕНЯЯ при этом ни одного файла. Ноль конфликтов, ноль правок — просто
теперь есть от чего считать. Со второго архива слияния идут обычным порядком.

Мусор, накопленный прошлыми копированиями, первое слияние не тронет: этих
файлов нет ни в одном снимке, значит для git они ваши. Отчёт назовёт их
поимённо — снести один раз руками.

Самопроверка (создаёт временные репозитории, сеть не нужна):
    python3 tools/sync_zip.py --selftest
"""
import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime

# ─── ЗАПУЩЕНО ВТОРЫМ ПИТОНОМ? ───────────────────────────────────────────────
# На рабочей машине `python` — это python2, и он падал на первой же
# конструкции третьего питона: «SyntaxError: invalid syntax» под стрелкой,
# указывающей на `check=True`. Понять по такому сообщению, что дело в версии,
# невозможно. Поэтому файл целиком РАЗБИРАЕТСЯ вторым питоном (никаких
# f-строк, никаких именованных после *args, никакой распаковки в литералах —
# это проверяется глазами и AST'ом), а здесь мы просто перезапускаемся под
# третьим. Человеку ничего делать не надо.
if sys.version_info[0] < 3:
    try:
        os.execvp("python3", ["python3", os.path.abspath(__file__)] + sys.argv[1:])
    except Exception:
        sys.stderr.write(
            "Этот скрипт работает на Python 3, а запущен вторым "
            "(python --version = %s).\n"
            "Запустите так:\n    python3 %s <архив.zip>\n"
            % (sys.version.split()[0], os.path.basename(__file__)))
        sys.exit(1)

SNAPSHOT_BRANCH = "github-snapshot"

# По этим файлам архив опознаётся как «наш». Проверка дешёвая, а цена ошибки
# велика: развернуть поверх репозитория чужое дерево — это молча снести всё.
MARKERS = ("tools/dag_builder.py", "etlFolder")

# Области, про которые в отчёте говорится ОТДЕЛЬНО и всегда, даже когда слияние
# прошло без конфликта. Здесь живут ваши правки — структуры, запросы, конфиги
# линий, — и «слилось молча» тут не означает «я этого хотел».
YOURS = ("etlFolder/", "dags/")


class Otkaz(Exception):
    """Ошибка, которую пользователю показывают текстом, а не трассировкой."""


def find_repo(hint=None, archive=None):
    """Где рабочая копия.

    Ищем от ТЕКУЩЕГО каталога, а не от места скрипта: скрипт легко оказывается
    скопированным в домашнюю папку (так и вышло при первом запуске), и путь «на
    два уровня вверх от файла» указал бы в C:\\Users — не репозиторий вовсе.
    А промахнуться тут дорого: дальше идут коммиты и слияния.
    """
    if hint:
        top = hint
    else:
        try:
            p = subprocess.run(["git", "rev-parse", "--show-toplevel"],
                               capture_output=True, text=True)
        except OSError:
            raise Otkaz(
                "Не нашёл команду git. Она нужна: весь перенос делается\n"
                "слиянием, а не копированием файлов.\n"
                "Windows: поставьте Git for Windows и запускайте из его\n"
                "консоли — либо добавьте git в PATH.")
        top = (p.stdout or "").strip() if p.returncode == 0 else ""
        if not top:
            # запасной путь: рядом со скриптом, если он лежит в tools/
            here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            top = here if os.path.isdir(os.path.join(here, ".git")) else ""
    if not top or not os.path.exists(os.path.join(top, ".git")):
        near = guess_repos()
        text = ["Не нашёл рабочую копию репозитория.", ""]
        if near:
            text.append("Похоже, она вот здесь — запустите оттуда:")
            # В подсказке — ТОТ путь к архиву, который человек и набрал:
            # подставлять свой пример значит заставлять его читать команду
            # заново и мысленно её переписывать.
            for path in near:
                text.append("    cd {}".format(path))
                text.append("    python tools{}sync_zip.py {} --base".format(
                    os.sep, archive or "<архив.zip>"))
            text.append("")
            text.append("Либо указать явно: --root <путь>")
        else:
            text += [
                "И рядом её тоже нет. Два частых случая:",
                "",
                "1. Распакованный архив — это НЕ рабочая копия. Папка вида",
                "   Etl-main рядом с zip'ом содержит файлы, но не содержит",
                "   истории (.git), сливать в ней нечего. Распаковывать архив",
                "   вообще не нужно: скрипт делает это сам.",
                "",
                "2. Скрипт запускают не на том ПК. Работать он должен ТАМ, где",
                "   лежит клон, который вы пушите в gitea (dev-push.sh), —",
                "   архив достаточно принести туда на флешке.",
                "",
                "Если копия есть, но лежит в неожиданном месте, укажите её:",
                "    --root <путь к папке с .git>",
            ]
        raise Otkaz("\n".join(text))
    missing = [m for m in MARKERS if not os.path.exists(os.path.join(top, m))]
    if missing:
        raise Otkaz(
            "Каталог {} — репозиторий, но не наш: не нашёл {}.".format(
                top, ", ".join(missing)))
    return top


def is_repo(path):
    """Рабочая копия НАШЕГО проекта: git-каталог плюс опознавательные файлы.

    `.git` бывает и файлом (это worktree), поэтому exists, а не isdir.
    """
    if not path or not os.path.exists(os.path.join(path, ".git")):
        return False
    return all(os.path.exists(os.path.join(path, m)) for m in MARKERS)


def guess_repos(limit=8):
    """Поискать рабочую копию рядом — чтобы вместо «не нашёл» назвать, где она.

    Смотрим текущий каталог с родителями, домашнюю папку и корни дисков, на
    один уровень вглубь. Дальше не лезем: это подсказка, а не индексатор диска.
    """
    bases, found, seen = [], [], set()
    cwd = os.path.abspath(os.getcwd())
    node = cwd
    while True:                                   # сам каталог и все родители
        bases.append(node)
        parent = os.path.dirname(node)
        if parent == node:
            break
        node = parent
    bases.append(os.path.expanduser("~"))
    if os.name == "nt":
        bases += ["{}:\\".format(chr(c)) for c in range(ord("A"), ord("Z") + 1)
                  if os.path.isdir("{}:\\".format(chr(c)))]

    for base in list(bases):                      # и на один уровень вглубь
        try:
            for name in sorted(os.listdir(base)):
                if not name.startswith("."):
                    bases.append(os.path.join(base, name))
        except OSError:
            continue

    for path in bases:
        real = os.path.abspath(path)
        if real in seen:
            continue
        seen.add(real)
        try:
            if is_repo(real):
                found.append(real)
        except OSError:
            continue
        if len(found) >= limit:
            break
    return found


def find_archives(name=None, limit=10):
    """Поискать *.zip там, куда обычно монтируют флешку.

    Нужно из-за самой частой осечки: `/dev/sdc1` — это УСТРОЙСТВО, а не
    каталог. Файла по такому пути нет и быть не может, а сказать «нет такого
    каталога» мало: человеку нужна точка монтирования, а она у каждой системы
    своя (/media/<юзер>/<метка>, /run/media/…, /mnt/…).
    """
    if os.name == "nt":
        bases = ["{}:\\".format(chr(c)) for c in range(ord("A"), ord("Z") + 1)]
    else:
        bases = ["/media", "/run/media", "/mnt"]
        user = os.environ.get("USER") or ""
        if user:
            bases += ["/media/" + user, "/run/media/" + user]
        # На Astra флешку монтируют в /run/user/<uid>/media/<by-id-usb-…>:
        # ни /media, ни /run/media туда не ведут, и поиск проходил мимо.
        try:
            for entry in os.listdir("/run/user"):
                bases.append(os.path.join("/run/user", entry, "media"))
        except OSError:
            pass
    found, seen = [], set()
    # ДВА уровня вглубь, а не один: на Linux флешка монтируется в
    # /media/<юзер>/<метка> — один уровень доводит только до /media/<юзер>, и
    # поиск молча ничего не находит там, где архив лежит.
    for _ in range(2):
        for base in list(bases):
            try:
                for entry in sorted(os.listdir(base)):
                    path = os.path.join(base, entry)
                    if os.path.isdir(path) and path not in bases:
                        bases.append(path)
            except OSError:
                continue
    for base in bases:
        if base in seen or not os.path.isdir(base):
            continue
        seen.add(base)
        try:
            for entry in sorted(os.listdir(base)):
                if not entry.lower().endswith(".zip"):
                    continue
                if name and entry.lower() != name.lower():
                    continue
                found.append(os.path.join(base, entry))
                if len(found) >= limit:
                    return found
        except OSError:
            continue
    return found


def check_archive(path):
    """Понятный отказ вместо трассировки: путь к архиву — самая частая осечка,
    а FileNotFoundError из недр zipfile об этом не говорит ничего."""
    if os.path.isfile(path):
        return path
    folder = os.path.dirname(os.path.abspath(path)) or "."
    lines = ["Не нашёл архив: {}".format(path)]

    if path.startswith("/dev/"):
        lines += [
            "",
            "/dev/sdc1 — это УСТРОЙСТВО, а не каталог: файлов по такому пути",
            "не бывает. Нужна точка монтирования — куда система подключила",
            "флешку. Посмотреть:",
            "    lsblk -o NAME,LABEL,MOUNTPOINT",
            "или просто открыть флешку в файловом менеджере и глянуть адрес.",
        ]
    elif os.path.isdir(folder):
        zips = sorted(f for f in os.listdir(folder) if f.lower().endswith(".zip"))
        lines.append("В каталоге {} {}".format(
            folder, "лежат: " + ", ".join(zips) if zips else "архивов (*.zip) нет."))
    else:
        lines.append("Каталога {} нет.".format(folder))

    near = find_archives(os.path.basename(path)) or find_archives()
    if near:
        lines += ["", "Нашёл архивы на подключённых носителях:"]
        for item in near:
            lines.append("    {}".format(item))
    raise Otkaz("\n".join(lines))


def _git(root, *args, **kw):
    # Именованные после *args и распаковка в литерале — синтаксис Python 3, а
    # файл обязан РАЗБИРАТЬСЯ вторым питоном: иначе проверка версии ниже не
    # успевает выполниться и человек получает SyntaxError вместо объяснения.
    check = kw.get("check", True)
    timeout = kw.get("timeout", 300)
    env = dict(os.environ, GIT_TERMINAL_PROMPT="0", LC_ALL="C.UTF-8")
    p = subprocess.run(["git", "-C", root] + list(args), capture_output=True,
                       text=True, env=env, timeout=timeout)
    if check and p.returncode != 0:
        raise RuntimeError("git {}:\n{}".format(
            " ".join(args), (p.stderr or p.stdout).strip()))
    return p.returncode, (p.stdout or "").strip(), (p.stderr or "").strip()


def extract(archive, into):
    """Распаковать архив и вернуть каталог с деревом репозитория.

    В архиве GitHub всё лежит внутри одной папки вида `Etl-main/` — её и
    разворачиваем. Заодно проверяем, что дерево наше: `zip` с чужим проектом,
    развёрнутый поверх, снёс бы всё молча.
    """
    with zipfile.ZipFile(archive) as z:
        names = z.namelist()
        if not names:
            raise ValueError("Архив пуст.")
        # Путь с '..' или абсолютный — вылезет за каталог назначения.
        for n in names:
            if n.startswith("/") or ".." in n.replace("\\", "/").split("/"):
                raise ValueError("Подозрительный путь в архиве: {!r}".format(n))
        z.extractall(into)
        # zipfile НЕ восстанавливает права: всё распаковывается как 644, и бит
        # исполняемости теряется. В архиве он есть (git archive кладёт его в
        # external_attr), и без этой строки два десятка скриптов deploy/*.sh
        # выглядели бы изменёнными в КАЖДОМ переносе — «поменялся режим
        # 100755 -> 100644». Отчёт, где четверть строк — шум, перестают читать.
        for info in z.infolist():
            mode = (info.external_attr >> 16) & 0o7777
            if mode and not info.is_dir():
                target = os.path.join(into, info.filename)
                if os.path.exists(target):
                    os.chmod(target, mode)

    entries = [e for e in os.listdir(into) if not e.startswith(".")]
    tree = os.path.join(into, entries[0]) if len(entries) == 1 and os.path.isdir(
        os.path.join(into, entries[0])) else into

    missing = [m for m in MARKERS if not os.path.exists(os.path.join(tree, m))]
    if missing:
        raise ValueError(
            "Это не похоже на архив нашего репозитория — не нашёл: "
            + ", ".join(missing)
            + ". Разворачивать его поверх рабочей копии нельзя.")
    return tree


def import_snapshot(tree, root, branch=SNAPSHOT_BRANCH, note=""):
    """Сделать распакованное дерево КОММИТОМ на ветке-снимке.

    Собирается через отдельный индекс: рабочая копия при этом не трогается
    вовсе, так что импорт архива безопасен даже поверх незакоммиченных правок.
    """
    parent = _git(root, "rev-parse", "-q", "--verify", branch, check=False)[1]
    with tempfile.NamedTemporaryFile(prefix="etl-sync-index-", delete=False) as fp:
        index = fp.name
    os.unlink(index)                       # git создаст его сам
    try:
        env = dict(os.environ, GIT_INDEX_FILE=index, GIT_TERMINAL_PROMPT="0")
        # --work-tree подсовывает гиту распакованное дерево вместо рабочей копии
        cmd = ["git", "-C", root, "--work-tree", tree]
        p = subprocess.run(cmd + ["add", "-A"], capture_output=True, text=True, env=env)
        if p.returncode != 0:
            raise RuntimeError("git add: {}".format((p.stderr or p.stdout).strip()))
        p = subprocess.run(cmd + ["write-tree"], capture_output=True, text=True, env=env)
        if p.returncode != 0:
            raise RuntimeError("git write-tree: {}".format((p.stderr or p.stdout).strip()))
        tree_sha = p.stdout.strip()

        message = "снимок GitHub {}".format(
            datetime.now().strftime("%Y-%m-%d %H:%M"))
        if note:
            message += "\n\n{}".format(note)
        args = ["commit-tree", tree_sha, "-m", message]
        if parent:
            args += ["-p", parent]
        commit = _git(root, *args)[1]
        _git(root, "update-ref", "refs/heads/{}".format(branch), commit)
        return commit, parent
    finally:
        if os.path.exists(index):
            os.unlink(index)


def _names(root, *args):
    out = _git(root, *args)[1]
    return [l for l in out.splitlines() if l.strip()]


def report(root, commit, parent, branch=SNAPSHOT_BRANCH):
    """Что принёс архив и что из этого требует вашего внимания."""
    head = _git(root, "rev-parse", "HEAD")[1]
    out = {"commit": commit, "parent": parent, "first": not parent,
           "added": [], "changed": [], "removed": [], "yours": [],
           "only_here": [], "dirty": _git(root, "status", "--porcelain")[1]}

    if parent:
        for line in _names(root, "diff", "--name-status", parent, commit):
            status, _, path = line.partition("\t")
            path = path.strip()
            if status.startswith("A"):
                out["added"].append(path)
            elif status.startswith("D"):
                out["removed"].append(path)
            else:
                out["changed"].append(path)
    else:
        # Базы нет: сравнивать архив можно только с текущим деревом. Тогда
        # «удалено» означает «есть у вас, нет в архиве» — а это и мусор от
        # прошлых копирований, и ваши собственные файлы вперемешку. Поэтому в
        # первом запуске мы ничего не удаляем и не сливаем, только показываем.
        for line in _names(root, "diff", "--name-status", head, commit):
            status, _, path = line.partition("\t")
            path = path.strip()
            if status.startswith("A"):
                out["added"].append(path)
            elif status.startswith("D"):
                out["only_here"].append(path)
            else:
                out["changed"].append(path)

    touched = out["added"] + out["changed"] + out["removed"]
    out["yours"] = sorted(p for p in touched if p.startswith(YOURS))
    return out


def render(rep):
    """Отчёт человеку. Пишется так, чтобы читался сверху вниз и заканчивался
    ответом на вопрос «что мне теперь делать»."""
    lines = []
    add = lines.append
    if rep["first"]:
        add("ПЕРВЫЙ ЗАПУСК: базы для сравнения ещё нет, поэтому ничего не сливаю.")
        add("Ниже — чем принесённый архив отличается от вашего дерева.")
    else:
        add("Прошлый архив: {}   нынешний: {}".format(
            rep["parent"][:9], rep["commit"][:9]))
    add("")
    add("  новых файлов:      {}".format(len(rep["added"])))
    add("  изменённых:        {}".format(len(rep["changed"])))
    if rep["first"]:
        add("  есть у вас, нет в архиве: {}".format(len(rep["only_here"]))
            + "   <- мусор от копирований И ваши файлы вперемешку")
    else:
        add("  УДАЛЁННЫХ:         {}".format(len(rep["removed"]))
            + "   <- то, чего копирование никогда не делало")

    if rep["removed"]:
        add("")
        add("Будут удалены:")
        for p in rep["removed"][:40]:
            add("    {}".format(p))
        if len(rep["removed"]) > 40:
            add("    … и ещё {}".format(len(rep["removed"]) - 40))

    if rep["yours"]:
        add("")
        add("ВАШИ ОБЛАСТИ — посмотрите глазами, здесь бывают ваши собственные правки:")
        for p in rep["yours"]:
            add("    {}".format(p))
        add("  (структуры, запросы и конфиги линий у вас могут отличаться от моих;")
        add("   слияние без конфликта тут не означает, что этого хотели)")

    add("")
    if rep["dirty"]:
        add("!! В рабочей копии есть несохранённые правки — слияние их не тронет,")
        add("   но разбираться потом будет сложнее. Лучше сначала закоммитить.")
    if rep["first"]:
        add("Дальше: разберите список «есть у вас, нет в архиве». Что из него мусор")
        add("прошлых копирований — снесите (git rm), что ваше — оставьте. Со")
        add("следующего архива всё это делает слияние само.")
    else:
        add("Дальше: слияние ветки-снимка в вашу. Конфликты — там, где файл")
        add("поменялся и у меня, и у вас; git оставит их помеченными в файлах.")
    return "\n".join(lines)


def join(root, branch=SNAPSHOT_BRANCH):
    """Пришить ветку-снимок к истории, НЕ МЕНЯЯ рабочего дерева.

    Первый снимок — коммит без родителей, общей истории с вашей веткой у него
    нет, и обычное слияние git отклоняет («refusing to merge unrelated
    histories»). Стратегия `ours` записывает снимок в предки, а дерево берёт
    ваше целиком: ни один файл не меняется. Смысл ровно один — чтобы у
    СЛЕДУЮЩЕГО слияния появилась база, от которой считать.

    Пришивать надо именно так, а не делать первый снимок потомком вашего HEAD:
    тогда базой стало бы ВАШЕ дерево, и первое же слияние снесло бы всё, чего
    нет в моём архиве, — то есть ваши структуры и запросы.
    """
    rc, out, err = _git(root, "merge", "-s", "ours", "--allow-unrelated-histories",
                        "--no-ff", branch,
                        "-m", "база для переносов с GitHub ({})".format(branch),
                        check=False)
    return rc == 0, (out + "\n" + err).strip()


def merge(root, branch=SNAPSHOT_BRANCH):
    """Слить ветку-снимок в текущую. Возвращает (ok, лог)."""
    rc, out, err = _git(root, "merge", "--no-ff", branch,
                        "-m", "перенос с GitHub ({})".format(branch), check=False)
    log = (out + "\n" + err).strip()
    if rc == 0:
        return True, log
    conflicts = _names(root, "diff", "--name-only", "--diff-filter=U")
    if conflicts:
        log += ("\n\nКонфликты (поменялось и у меня, и у вас):\n"
                + "\n".join("    " + c for c in conflicts)
                + "\n\nРазберите их в редакторе, затем:\n"
                  "    git add <файл> ...\n"
                  "    git commit\n"
                  "Передумали — откат: git merge --abort")
    return False, log


def main(argv):
    ap = argparse.ArgumentParser(
        description="Перенос правок с GitHub через zip — слиянием, а не копированием")
    ap.add_argument("archive", nargs="?", help="zip, скачанный с GitHub")
    ap.add_argument("--root", default=None,
                    help="папка репозитория (по умолчанию — та, из которой запущено)")
    ap.add_argument("--branch", default=SNAPSHOT_BRANCH)
    ap.add_argument("--dry-run", action="store_true", help="только отчёт")
    ap.add_argument("--base", action="store_true",
                    help="первый запуск: завести ветку-снимок, не сливая")
    ap.add_argument("--yes", action="store_true", help="не спрашивать подтверждения")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args(argv)

    if args.selftest:
        return _selftest()
    if not args.archive:
        ap.error("не задан архив")

    try:
        root = find_repo(args.root, args.archive)
        archive = check_archive(args.archive)
    except Otkaz as err:
        print(err)
        return 1

    tmp = tempfile.mkdtemp(prefix="etl-sync-")
    try:
        tree = extract(archive, tmp)
        commit, parent = import_snapshot(tree, root, args.branch,
                                         note=os.path.basename(archive))
        rep = report(root, commit, parent, args.branch)
        print(render(rep))
        if args.dry_run:
            print("\nВетка-снимок {} обновлена. Слияние не делалось.".format(args.branch))
            return 0
        if rep["first"] or args.base:
            if rep["dirty"]:
                print("\n!! Сначала закоммитьте или отмените несохранённые правки:"
                      "\n   пришивание базы — это коммит, а с грязным деревом git его не сделает.")
                return 1
            ok, log = join(root, args.branch)
            print()
            print(log)
            print("\nБаза заведена: дерево не изменилось ни на один файл, но у")
            print("следующего архива появилось, с чем сравниваться.")
            return 0 if ok else 2
        if not args.yes:
            print()
            if input("Сливать? (y/N) ").strip().lower() not in ("y", "д"):
                print("Отменено. Ветка-снимок обновлена, слить можно потом:")
                print("    git merge --no-ff {}".format(args.branch))
                return 1
        ok, log = merge(root, args.branch)
        print(log)
        return 0 if ok else 2
    except (Otkaz, ValueError) as err:
        print(err)
        return 1
    except RuntimeError as err:
        print("git отказал:\n{}".format(err))
        return 1
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# ─────────────────────────── самопроверка ───────────────────────────

def _mkzip(path, files, top="Etl-main"):
    with zipfile.ZipFile(path, "w") as z:
        for name, content in files.items():
            z.writestr("{}/{}".format(top, name), content)


def _selftest():
    """Проверка на настоящих репозиториях во временных каталогах."""
    with tempfile.TemporaryDirectory() as tmp:
        repo = os.path.join(tmp, "repo")
        os.makedirs(repo)
        _git(repo, "init", "-q", "-b", "test")
        _git(repo, "config", "user.email", "t@t")
        _git(repo, "config", "user.name", "t")

        # исходное дерево: мой код + файл, который потом исчезнет, + ЧУЖОЙ файл,
        # которого в моих архивах нет никогда
        start = {
            "tools/dag_builder.py": "# v1\n",
            "tools/webui/dist/assets/index-OLD.js": "old\n",
            "etlFolder/config.d/LINE.json": '{"a": 1}\n',
            "etlFolder/queries/customQueries/MOJ.sql": "SELECT 1 FROM моё\n",
        }
        for name, content in start.items():
            os.makedirs(os.path.join(repo, os.path.dirname(name)), exist_ok=True)
            with open(os.path.join(repo, name), "w", encoding="utf-8") as fp:
                fp.write(content)
        _git(repo, "add", "-A")
        _git(repo, "commit", "-q", "-m", "исходное")

        # ── архив №1: то же самое, заводим базу ──────────────────────────────
        zip1 = os.path.join(tmp, "a1.zip")
        _mkzip(zip1, {k: v for k, v in start.items()
                      if k != "etlFolder/queries/customQueries/MOJ.sql"})
        before = _git(repo, "rev-parse", "HEAD^{tree}")[1]
        assert main([zip1, "--root", repo, "--base"]) == 0
        base = _git(repo, "rev-parse", SNAPSHOT_BRANCH)[1]
        assert base
        # Пришивание базы обязано не менять НИ ОДНОГО файла: иначе первый же
        # перенос переписал бы рабочую копию тем, что я о ней думаю.
        assert _git(repo, "rev-parse", "HEAD^{tree}")[1] == before, \
            "пришивание базы изменило дерево"
        assert _git(repo, "status", "--porcelain")[1] == "", "остались правки"
        # …и при этом снимок стал предком: иначе следующее слияние снова
        # упрётся в «unrelated histories»
        assert _git(repo, "merge-base", "--is-ancestor", base, "HEAD",
                    check=False)[0] == 0, "снимок не попал в предки"

        # ── архив №2: файл удалён, код изменён, добавлен новый ───────────────
        second = {
            "tools/dag_builder.py": "# v2\n",
            "tools/webui/dist/assets/index-NEW.js": "new\n",
            "etlFolder/config.d/LINE.json": '{"a": 2}\n',
        }
        zip2 = os.path.join(tmp, "a2.zip")
        _mkzip(zip2, second)
        rc = main([zip2, "--root", repo, "--yes"])
        assert rc == 0, rc

        def exists(p):
            return os.path.exists(os.path.join(repo, p))

        # 1. УДАЛЕНИЕ доехало — то, чего копирование не делало никогда
        assert not exists("tools/webui/dist/assets/index-OLD.js"), "мусор не удалён"
        # 2. новый файл появился
        assert exists("tools/webui/dist/assets/index-NEW.js")
        # 3. изменение применилось
        with open(os.path.join(repo, "tools/dag_builder.py"), encoding="utf-8") as fp:
            assert fp.read() == "# v2\n"
        # 4. ЧУЖОЙ файл цел: его не было ни в одном архиве, значит он не мой
        assert exists("etlFolder/queries/customQueries/MOJ.sql"), \
            "слияние снесло пользовательский файл — ровно то, чего нельзя"

        # ── архив №3 против ВСТРЕЧНОЙ правки: обязан быть конфликт ───────────
        with open(os.path.join(repo, "etlFolder/config.d/LINE.json"), "w",
                  encoding="utf-8") as fp:
            fp.write('{"a": 2, "моё": true}\n')
        _git(repo, "commit", "-qam", "моя правка конфига")
        zip3 = os.path.join(tmp, "a3.zip")
        _mkzip(zip3, dict(second, **{"etlFolder/config.d/LINE.json": '{"a": 3}\n'}))
        rc = main([zip3, "--root", repo, "--yes"])
        assert rc == 2, "ожидался конфликт, получено {}".format(rc)
        conflicts = _names(repo, "diff", "--name-only", "--diff-filter=U")
        assert conflicts == ["etlFolder/config.d/LINE.json"], conflicts
        _git(repo, "merge", "--abort")

        # ── отчёт называет ваши области отдельно ─────────────────────────────
        with tempfile.TemporaryDirectory() as t2:
            tree = extract(zip3, t2)
            commit, parent = import_snapshot(tree, repo, "проба")
            rep = report(repo, commit, parent, "проба")
            assert "etlFolder/config.d/LINE.json" in rep["yours"], rep
            text = render(rep)
            assert "ВАШИ ОБЛАСТИ" in text, text

        # ── бит исполняемости переживает перенос ─────────────────────────────
        # zipfile его не восстанавливает, и без правки два десятка скриптов
        # deploy/*.sh «менялись» в каждом переносе — шум, из-за которого отчёт
        # перестают читать.
        exe = os.path.join(tmp, "exe.zip")
        with zipfile.ZipFile(exe, "w") as z:
            for name, mode in (("tools/dag_builder.py", 0o644),
                               ("etlFolder/x", 0o644),
                               ("deploy/run.sh", 0o755)):
                info = zipfile.ZipInfo("Etl-main/" + name)
                info.external_attr = mode << 16
                z.writestr(info, "x\n")
        with tempfile.TemporaryDirectory() as t5:
            tree = extract(exe, t5)
            got = os.stat(os.path.join(tree, "deploy/run.sh")).st_mode & 0o777
            assert got == 0o755, oct(got)
            got = os.stat(os.path.join(tree, "tools/dag_builder.py")).st_mode & 0o777
            assert got == 0o644, oct(got)

        # ── чужой архив разворачивать нельзя ─────────────────────────────────
        alien = os.path.join(tmp, "alien.zip")
        _mkzip(alien, {"README.md": "чужой проект\n"}, top="other")
        with tempfile.TemporaryDirectory() as t3:
            try:
                extract(alien, t3)
            except ValueError as e:
                assert "не похоже на архив" in str(e), e
            else:
                raise AssertionError("чужой архив принят — так сносят рабочую копию")

        # ── путь с '..' в архиве ─────────────────────────────────────────────
        evil = os.path.join(tmp, "evil.zip")
        with zipfile.ZipFile(evil, "w") as z:
            z.writestr("../вылез.txt", "x")
        with tempfile.TemporaryDirectory() as t4:
            try:
                extract(evil, t4)
            except ValueError as e:
                assert "Подозрительный путь" in str(e), e
            else:
                raise AssertionError("путь с '..' принят")

    print("sync_zip selftest OK")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
