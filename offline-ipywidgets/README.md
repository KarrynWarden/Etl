# Офлайн-установка ipywidgets

Набор wheel-файлов для установки `ipywidgets` на сервер Jupyter **без доступа в интернет**
(когда `pip install` блокируется внутренней сетью).

Собрано под целевое окружение:

- **Python 3.10** (`cp310`, платформа `manylinux_2_17_x86_64` / `any`)
- **pip 23.3.1**
- **JupyterLab 4.0.4**

Версия пакета: **ipywidgets 8.1.8** (совместима с JupyterLab 4.x).

## Содержимое

```
offline-ipywidgets/
├── wheels/                       # все .whl: ipywidgets + зависимости
├── install.sh                    # скрипт офлайн-установки
├── ipywidgets-offline.tar.gz     # тот же набор одним архивом (удобно перебросить через телефон)
└── README.md
```

## Как доставить файлы на сервер

Сервер с Jupyter интернета не имеет, поэтому файлы нужно туда перенести вручную. Любой из вариантов:

1. **Один архив (проще всего для переброски через телефон/файлообменник):**
   скачайте `ipywidgets-offline.tar.gz`, перекиньте на ПК, затем на сервер.
2. **Через git** на машине, у которой есть доступ к GitHub: склонируйте/обновите репозиторий
   и возьмите папку `offline-ipywidgets/`.
3. **Через веб-интерфейс JupyterLab:** меню Upload — загрузите архив или отдельные `.whl`.

## Установка на сервере (без интернета)

### Вариант А — из архива

```bash
tar -xzf ipywidgets-offline.tar.gz
cd offline-ipywidgets
bash install.sh
```

### Вариант Б — из папки wheels напрямую

```bash
python -m pip install --no-index --find-links=./wheels ipywidgets
```

Флаг `--no-index` запрещает pip выходить в сеть, `--find-links` указывает на локальную папку.
Уже установленные подходящие зависимости (ipython, traitlets и т.д.) переустановлены не будут.

## Проверка

```bash
python -c "import ipywidgets; print(ipywidgets.__version__)"
```

После установки **перезапустите ядро и сервер JupyterLab**. В связке JupyterLab 4 + ipywidgets 8
расширение `jupyterlab_widgets` ставится как обычный pip-пакет — отдельные команды
`jupyter labextension install` не нужны.

Быстрый тест в ноутбуке:

```python
import ipywidgets as widgets
widgets.IntSlider()
```

## Если Python на сервере не 3.10

Все пакеты в наборе — чистый Python (`py3-none-any` / `py2.py3-none-any`), поэтому
они подойдут и для других версий Python 3. Исключение — если на сервере уже стоит
несовместимая версия зависимости; в этом случае сообщите версию Python на сервере,
и я пересоберу набор точно под неё.
