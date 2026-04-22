# CBR Bank Reports Pipeline

Production-grade пайплайн для массового сбора отчетности кредитных организаций с сайта Банка России.

## Архитектура

Очереди Celery:
- `bootstrap` — стартовая инициализация и постановка задач обхода страниц банков.
- `fetch` — IO-bound загрузка HTML.
- `parse` — CPU-bound разбор HTML в структурированные данные.
- `aggregate` — финальная агрегация и формирование единого JSON.

Поток данных:
1. `start_bootstrap` скачивает FullCoList, извлекает банки, фильтрует активные лицензии.
2. Для каждого банка ставится `fetch_page(page_type=reports_index)`.
3. `parse_page(reports_index)` извлекает все формы/даты/ссылки и ставит задачи fetch на страницы отчетов.
4. `parse_page(report_page)` парсит таблицы и метаданные, сохраняет структуру в SQLite.
5. `build_final_json` собирает dedup-JSON в `data/parsed/all_banks_reports.json`.

## Особенности production-grade реализации

- Idempotent задачи через fingerprint URL (`sha256`) + state в SQLite.
- Retry + exponential backoff для fetch.
- Дедупликация на уровнях:
  - `fetched_pages.url_fp`
  - `report_index_items.report_url_fp`
  - `parsed_reports.report_url_fp`
- Raw HTML сохраняется на диск: `data/raw_html/<ogrn>/<url_fp>.html`.
- Parse не выполняет network I/O.
- Fetch не выполняет тяжелый HTML parsing.
- Можно независимо масштабировать воркеры `fetch` и `parse`.
- Перезапуск безопасен: уже загруженные/распарсенные страницы не обрабатываются повторно.

## Структура проекта

```text
.
├── app
│   ├── cli.py
│   ├── core
│   │   ├── celery_app.py
│   │   ├── config.py
│   │   └── logging.py
│   ├── parsers
│   │   ├── bank_list_parser.py
│   │   ├── report_page_parser.py
│   │   └── reports_index_parser.py
│   ├── storage
│   │   ├── models.py
│   │   └── state.py
│   ├── tasks
│   │   ├── aggregate.py
│   │   ├── bootstrap.py
│   │   ├── fetch.py
│   │   └── parse.py
│   └── utils
│       ├── fingerprint.py
│       └── http_client.py
├── data
│   ├── parsed
│   └── raw_html
├── scripts
│   ├── progress.py
│   └── wait_and_aggregate.py
├── docker-compose.yml
├── Dockerfile
├── entrypoint.sh
├── pyproject.toml
└── requirements.txt
```

## Быстрый запуск (demo на одном банке)

```bash
export FULL_RUN=0
export DEMO_OGRN=1022200525841

docker compose up -d --build redis bootstrap-worker fetch-worker parse-worker aggregate-worker runner

docker compose exec runner python -m app.cli bootstrap

docker compose exec runner python scripts/progress.py

docker compose exec runner python -m app.cli aggregate
```

## Full run

```bash
unset DEMO_OGRN
export FULL_RUN=1

docker compose up -d --build redis bootstrap-worker fetch-worker parse-worker aggregate-worker runner
docker compose exec runner python -m app.cli bootstrap

# ждать накопления parsed и затем:
docker compose exec runner python scripts/wait_and_aggregate.py
```

## Масштабирование

```bash
# Пример: добавить fetch/parse мощность
FETCH_WORKER_CONCURRENCY=64 PARSE_WORKER_CONCURRENCY=8 docker compose up -d --scale fetch-worker=3 --scale parse-worker=2
```

## Прогресс и артефакты

- sqlite state: `data/state.db`
- raw html cache: `data/raw_html/`
- final json: `data/parsed/all_banks_reports.json`

Прогресс:
```bash
docker compose exec runner python scripts/progress.py
```

## Troubleshooting

### `sqlite3.OperationalError: attempt to write a readonly database`
Если директория `data/` была создана в другом режиме прав, удалите/исправьте права и пересоберите:

```bash
rm -f data/state.db
chmod -R u+rwX data

docker compose down -v
docker compose up -d --build
```
