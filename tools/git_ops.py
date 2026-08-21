#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Версии, откат и выкладка на прод — то, что конструктор делает с ГИТОМ.

Отдельно от dag_builder намеренно: тот собирает файлы линий и про ветки ничего
знать не должен. Здесь наоборот — ни одной строчки про линии, только состояния
репозитория.

Топология (deploy/README-prod.md):

    gitea ── ветка test ──► /opt/airflow-test/etl.git ──► airflow-test (:8082)
          └─ ветка prod ──► /opt/airflow-prod/etl.git ──► прод (:8080)

Конструктор живёт в клоне на тестовом сервере и пушит в origin текущую ветку —
то есть в test. Прод собирается ОТДЕЛЬНО: deploy/deploy-prod.sh делает коммит
поверх прода с деревом теста, вешает тег prod-ГГГГММДД-ЧЧММ и пушит. Откат
прода — выкладка предыдущего тега (FROM=prod-...). Здесь этот скрипт
ЗАПУСКАЕТСЯ, а не переписывается: две реализации выкладки уже расходились
однажды (см. «Почему гейт именно в pre-receive» в README) и стоили упавшего
прода.

Откат теста — своя механика, но правило то же, что у прода: НОВЫЙ КОММИТ
ПОВЕРХ. История растёт вперёд, push остаётся fast-forward, ничей клон не
ломается. reset --hard с force-push здесь не предлагается: у теста есть
клоны (dev-PC, test-src, сам конструктор), и переписанная история чинится
руками на каждом из них.

Самопроверка (без сети и без прода):  python3 tools/git_ops.py --selftest
"""
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tools import dag_builder as B  # noqa: E402

# Что конструктор считает «своим». Откат по умолчанию ограничен этим: код
# (Functions/, Src/, tools/) — не его хозяйство, а вдобавок из tools/ прямо
# сейчас исполняется он сам, и подменять его себе под ногами по кнопке в
# браузере — не то, чего ждёт нажимающий.
AREAS = B.GIT_AREAS                      # ("etlFolder", "dags")

# Разделитель полей в git log: \x00 не встречается в теме коммита, в отличие
# от любого печатного символа, который кто-нибудь однажды напишет.
_FMT = "%H%x00%h%x00%aI%x00%an%x00%D%x00%s"


def _git(root, *args, timeout=120):
    """(rc, stdout, stderr). Именно раздельно: stdout здесь РАЗБИРАЕТСЯ, и
    примешанное к нему предупреждение git'а (detached HEAD, hints) сломало бы
    разбор молча."""
    env = dict(os.environ, GIT_TERMINAL_PROMPT="0", LC_ALL="C.UTF-8")
    try:
        p = subprocess.run(["git", "-C", root, *args], capture_output=True,
                           text=True, env=env, timeout=timeout)
    except subprocess.TimeoutExpired:
        return 124, "", f"git {args[0]}: превышен таймаут ({timeout}с)"
    except OSError as e:
        return 127, "", str(e)
    return p.returncode, p.stdout or "", p.stderr or ""


def _parse_log(text):
    out = []
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split("\x00")
        if len(parts) < 6:
            continue
        sha, short, date, author, refs, subject = parts[:6]
        tags = [r.strip()[5:] for r in refs.split(",") if r.strip().startswith("tag: ")]
        out.append({"sha": sha, "short": short, "date": date, "author": author,
                    "subject": subject, "refs": refs, "tags": tags})
    return out


def versions(ref="HEAD", limit=40, root=ROOT, paths=None):
    """Список версий ветки: что и когда менялось.

    paths — ограничить историю областями конструктора: коммит, тронувший только
    Functions/, в списке «до какой версии откатить линии» лишний, он ничего не
    менял из того, что откат вернёт.
    """
    args = ["log", f"-n{int(limit)}", f"--format={_FMT}", ref]
    if paths:
        args += ["--", *paths]
    rc, out, err = _git(root, *args)
    if rc != 0:
        raise RuntimeError(f"git log не удался: {err.strip() or out.strip()}")
    return _parse_log(out)


def prod_tags(limit=40, root=ROOT):
    """Выкладки прода — теги prod-*, новые сверху. Это и есть «версии прода»:
    каждая выкладка ставит такой тег (deploy/deploy-prod.sh)."""
    rc, out, err = _git(root, "for-each-ref", "--sort=-creatordate",
                        f"--count={int(limit)}",
                        "--format=%(refname:short)%x00%(creatordate:iso-strict)"
                        "%x00%(objectname:short)%x00%(contents:subject)",
                        "refs/tags/prod-*")
    if rc != 0:
        raise RuntimeError(f"git for-each-ref не удался: {err.strip()}")
    tags = []
    for line in out.splitlines():
        parts = line.split("\x00")
        if len(parts) >= 4:
            tags.append({"tag": parts[0], "date": parts[1],
                         "short": parts[2], "subject": parts[3]})
    return tags


def _changed(root, ref, areas):
    """Пути, которыми дерево `ref` отличается от рабочего HEAD, в областях."""
    rc, out, err = _git(root, "diff", "--name-only", "HEAD", ref, "--", *areas)
    if rc != 0:
        raise RuntimeError(f"git diff не удался: {err.strip() or out.strip()}")
    return [p for p in out.splitlines() if p.strip()]


def _blob(root, ref, path):
    rc, out, _err = _git(root, "show", f"{ref}:{path}")
    return out if rc == 0 else None


def rollback_plan(ref, areas=AREAS, root=ROOT):
    """Что вернёт откат до версии `ref`. Ничего не меняет.

    Возвращает {ref, resolved, subject, files, remove, dirty, areas}:
      files  — [(путь, содержимое ТОЙ версии)] — их перезапишем;
      remove — файлы, которых в той версии не было, — их удалим;
      dirty  — несохранённые правки в областях (откат поверх них запрещён:
               иначе непонятно, что вернулось из версии, а что осталось от
               недоделанной работы).
    """
    rc, out, err = _git(root, "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}")
    resolved = out.strip()
    if rc != 0 or not resolved:
        raise ValueError(f"Не нашёл версию {ref!r} — обнови список версий.")

    head = _git(root, "rev-parse", "HEAD")[1].strip()
    if resolved == head:
        return {"ref": ref, "resolved": resolved, "subject": "", "files": [],
                "remove": [], "dirty": "", "areas": list(areas),
                "note": "Рабочая копия уже в этом состоянии."}

    info = _parse_log(_git(root, "log", "-1", f"--format={_FMT}", resolved)[1])
    files, remove = [], []
    for path in _changed(root, resolved, areas):
        content = _blob(root, resolved, path)
        if content is None:
            remove.append(path)          # в той версии файла не было
        else:
            files.append((path, content))
    return {"ref": ref, "resolved": resolved,
            "subject": info[0]["subject"] if info else "",
            "date": info[0]["date"] if info else "",
            "files": files, "remove": remove,
            "dirty": B.git_status_short(root, areas),
            "areas": list(areas)}


def rollback_apply(ref, areas=AREAS, root=ROOT, push=True):
    """Вернуть состояние версии `ref` НОВЫМ КОММИТОМ поверх текущей ветки.

    Не reset и не revert: дерево областей приводится к дереву той версии, и это
    коммитится как обычная правка. История растёт вперёд, push остаётся
    fast-forward — у теста есть клоны (dev-PC, test-src, сам конструктор), и
    переписанная история чинилась бы руками на каждом.
    """
    plan = rollback_plan(ref, areas, root)
    if plan.get("dirty"):
        raise ValueError(
            "В областях конструктора есть несохранённые правки — откат поверх "
            "них запрещён: потом не разберёшь, что вернулось из версии, а что "
            "осталось от недоделанного. Сначала запушьте или отмените их.\n"
            + plan["dirty"])
    if not plan["files"] and not plan["remove"]:
        return True, "Откатывать нечего: рабочая копия уже в этом состоянии."

    resolved = plan["resolved"]
    log = []
    # Тем же способом, что и точечная выкладка на прод (deploy/deploy-prod.sh):
    # файл есть в той версии — берём оттуда, нет — удаляем. Никаких reset.
    for path, _content in plan["files"]:
        rc, out, err = _git(root, "checkout", resolved, "--", path)
        if rc != 0:
            raise RuntimeError(f"git checkout {path}: {err.strip() or out.strip()}")
    for path in plan["remove"]:
        rc, out, err = _git(root, "rm", "-f", "--quiet", "--", path)
        if rc != 0:
            raise RuntimeError(f"git rm {path}: {err.strip() or out.strip()}")
    log.append(f"вернул файлов: {len(plan['files'])}, удалил: {len(plan['remove'])}")

    short = resolved[:9]
    message = (f"Откат до версии {short}"
               + (f": {plan['subject']}" if plan.get("subject") else "")
               + f"\n\nСостояние {', '.join(plan['areas'])} возвращено к {short}. "
                 f"История не переписана — это обычный коммит поверх.")
    run = B._git_runner(root)
    rc, out = run("add", "-A", "--", *plan["areas"])
    if rc != 0:
        raise RuntimeError(f"git add не удался:\n{out}")
    if not push:
        rc, out = run("commit", "-m", message)
        return rc == 0, "\n".join(log + [out.strip()])
    ok, out = B._commit_and_push(run, B.current_branch(root), message)
    return ok, "\n".join(log + [out])


# ─────────────────────────────── прод ───────────────────────────────

DEPLOY_SCRIPT = os.path.join("deploy", "deploy-prod.sh")


def prod_status(root=ROOT, probe=True):
    """Может ли конструктор выложить на прод — и если нет, то почему именно.

    Отвечает на вопрос, который иначе выясняют по ssh: у сервиса конструктора
    (jupyter:etldev) может просто не быть прав на /opt/airflow-prod/etl.git —
    прод-репо лежит в группе etlprod. Поэтому проверяем по шагам и называем
    первый непройденный, а не отвечаем «не получилось».
    """
    out = {"script": os.path.exists(os.path.join(root, DEPLOY_SCRIPT)),
           "remote": None, "reachable": None, "detail": "", "branch": None,
           "prod_head": None}
    out["branch"] = B.current_branch(root)
    rc, url, _err = _git(root, "remote", "get-url", "prod")
    if rc != 0:
        out["detail"] = ("В клоне нет remote 'prod'. Добавить:\n"
                         "  git remote add prod ssh://devel@airflow/opt/airflow-prod/etl.git")
        return out
    out["remote"] = url.strip()
    if not probe:
        return out
    # ls-remote — единственная дешёвая проверка, которая ходит по той же дороге,
    # что и push: права, ключи, сеть. Таймаут короткий: висящий ssh на кнопке
    # «показать состояние» хуже, чем честное «не достучался».
    rc, ls, err = _git(root, "ls-remote", "--heads", "prod", "prod", timeout=20)
    out["reachable"] = rc == 0
    if rc == 0:
        out["prod_head"] = (ls.split()[0][:9] if ls.strip() else None)
        out["detail"] = "" if ls.strip() else "На проде ветки prod ещё нет — это первая выкатка."
    else:
        out["detail"] = (err.strip() or ls.strip() or "неизвестная ошибка") + (
            "\n\nСкорее всего у сервиса конструктора (jupyter, группа etldev) нет "
            "доступа к /opt/airflow-prod/etl.git — оно в группе etlprod. Тогда "
            "выкладку делает devel по ssh, командой из блока ниже.")
    return out


def prod_run(args, root=ROOT, env=None, timeout=1800):
    """Запустить deploy/deploy-prod.sh и вернуть (rc, вывод).

    Скрипт ОДИН на всех: он же гоняет гейт check-dags.sh по тому дереву, что
    уедет, он же ставит тег, он же повторяет push при сетевых сбоях. Своя
    реализация здесь была бы третьей по счёту — две прошлые разошлись и уронили
    прод.
    """
    script = os.path.join(root, DEPLOY_SCRIPT)
    if not os.path.exists(script):
        raise FileNotFoundError(f"Нет {DEPLOY_SCRIPT} — обнови рабочую копию.")
    full = dict(os.environ, GIT_TERMINAL_PROMPT="0", **(env or {}))
    try:
        p = subprocess.run(["bash", script, *args], cwd=root, capture_output=True,
                           text=True, env=full, timeout=timeout)
    except subprocess.TimeoutExpired:
        return 124, (f"deploy-prod.sh: превышен таймаут ({timeout}с). Выкладка "
                     f"могла и уехать — проверьте состояние прода.")
    return p.returncode, ((p.stdout or "") + (p.stderr or "")).strip()


def prod_diff(root=ROOT):
    """Чем прод отличается от теста. Ничего не меняет."""
    rc, out = prod_run(["--diff"], root=root, timeout=120)
    return {"rc": rc, "log": out}


# Пароль на выкладку. Не защита — задержка.
#
# API слушает 127.0.0.1, наружу его отдаёт Apache со своей авторизацией, так
# что от чужого здесь пароль не спасает и не для того заведён. Он для СВОЕГО:
# «выложить на прод» — единственная кнопка в конструкторе, чьё последствие
# нельзя посмотреть в предпросмотре и отменить кнопкой рядом, и набрать слово
# руками значит на секунду остановиться и перечитать, что именно уезжает.
# Переопределяется переменной окружения — на случай, если однажды понадобится
# другое слово; хранить его в репозитории смысла нет ровно потому, что это не
# секрет.
PROD_PASSWORD = os.environ.get("ETL_PROD_PASSWORD", "admin")


def check_prod_password(value):
    if (value or "") != PROD_PASSWORD:
        raise PermissionError(
            "Неверное слово. Выкладка на прод — единственное действие, которое "
            "нельзя посмотреть в предпросмотре и отменить соседней кнопкой, "
            "поэтому она просит подтвердить словом.")


def prod_deploy(from_ref=None, lines=None, root=ROOT):
    """Выложить на прод. from_ref: что считать источником.

    По умолчанию origin/test — обычная выкладка. Тег prod-* в from_ref — это и
    есть ОТКАТ прода: дерево того тега уезжает новым коммитом поверх текущего
    прода, ровно как советует сам скрипт в конце своего вывода.
    """
    args = []
    for line in (lines or []):
        args += ["--line", str(line)]
    env = {"YES": "1"}
    if from_ref:
        env["FROM"] = str(from_ref)
    rc, out = prod_run(args, root=root, env=env)
    return {"rc": rc, "log": out, "ok": rc == 0}


# ─────────────────────────── самопроверка ───────────────────────────

def _selftest():
    # Разбор git log — на подделанном выводе: сеть и состояние репозитория тут
    # ни при чём, а формат ломается именно разбором.
    raw = ("abc123\x00abc123\x002026-08-20T15:00:00+05:00\x00Иванов\x00"
           "HEAD -> test, tag: prod-20260820-1500, origin/test\x00Правка линии\n"
           "def456\x00def456\x002026-08-19T10:00:00+05:00\x00Петров\x00\x00"
           "Ещё правка\n")
    got = _parse_log(raw)
    assert len(got) == 2, got
    assert got[0]["tags"] == ["prod-20260820-1500"], got[0]
    assert got[0]["subject"] == "Правка линии", got[0]
    assert got[1]["tags"] == [] and got[1]["author"] == "Петров", got[1]
    # тема с запятой и словом «tag:» внутри не должна выдумывать тег
    tricky = "a\x00a\x002026-01-01T00:00:00Z\x00Я\x00\x00Правка, tag: не тег\n"
    assert _parse_log(tricky)[0]["tags"] == [], _parse_log(tricky)

    # Область отката — только хозяйство конструктора. Расширится молча — и
    # кнопка в браузере начнёт подменять код, из которого сама и запущена.
    assert AREAS == ("etlFolder", "dags"), AREAS
    assert "tools" not in AREAS and "Functions" not in AREAS

    # На живом репозитории: версии читаются, план отката до HEAD пуст.
    if os.path.isdir(os.path.join(ROOT, ".git")):
        vs = versions(limit=3)
        assert vs and all(v["sha"] and v["date"] for v in vs), vs
        plan = rollback_plan("HEAD")
        assert plan["files"] == [] and plan["remove"] == [], plan
        # несуществующая версия — понятный отказ, а не трассировка
        try:
            rollback_plan("нет-такой-версии")
        except ValueError as e:
            assert "Не нашёл версию" in str(e), e
        else:
            raise AssertionError("ожидался отказ на несуществующей версии")
        # прод: без remote 'prod' отвечаем инструкцией, а не пустотой
        st = prod_status(probe=False)
        assert st["script"] is True, st
        if st["remote"] is None:
            assert "git remote add prod" in st["detail"], st

    print("git_ops selftest OK")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        _selftest()
    else:
        print(__doc__)
