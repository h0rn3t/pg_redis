# pg_redis

[![CI](https://github.com/h0rn3t/pg_redis/actions/workflows/ci.yml/badge.svg)](https://github.com/h0rn3t/pg_redis/actions/workflows/ci.yml)

`pg_redis` — розширення PostgreSQL, написане на C, що вбудовує Redis-подібне
сховище ключ-значення в пам'яті безпосередньо у PostgreSQL-бекенд і відкриває
його через SQL-функції зі схеми `pgredis` у вигляді іменованих лапками
ПРОПИСНИХ ідентифікаторів. (Бінарний файл розширення називається `pg_redis`;
PostgreSQL резервує префікс `pg_` для системних каталогів, тому SQL-схема
скорочена до `pgredis`.)

Це **не** повноцінна заміна Redis. Тут немає RESP-протоколу, pub/sub, потоків,
відсортованих множин і кластеризації. Натомість розширення надає невеликий,
добре протестований набір Redis-подібних команд — рядки, цілі числа, хеші,
списки, TTL, знімки та планувальник фонових завдань — що працюють всередині
того самого процесу, який обслуговує SQL-запити, з довговічністю даних на
основі WAL PostgreSQL.

## Для чого підходить

- Позбутися залежності від Redis для кешів, лічильників, ефемерного стану
  сесій або легких черг завдань.
- Мати один пул з'єднань, одне резервне копіювання, одну ACL-поверхню.
- Виконувати операції всередині поточної транзакції — `ROLLBACK` відкотить
  також копію запису в кеші.

## Для чого не підходить

- Потрібна сира пропускна здатність Redis або підмілісекундні реплікаційні
  кластери.
- Потрібен pub/sub, потоки, відсортовані множини, скриптинг або Redis-модулі.
- Потрібна область видимості ключів між бекендами в реальному часі без обходу
  через таблицю (в v0.1 простір ключів є сесійно-локальним; див. «Модель
  паралелізму» нижче).

## Можливості

- **Типи**: `string`, `int`, `hash`, `list`.
- **Команди**: `SET`, `GET`, `DEL`, `EXISTS`, `EXPIRE`, `TTL`, `INCR`, `DECR`,
  `HSET`, `HGET`, `HDEL`, `HEXISTS`, `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`,
  `KEYS`, `FLUSHALL`, `MEMORY_USAGE`, `STATS`, `INFO`, `SAVE`, `BGSAVE`,
  `BGREWRITEAOF`.
- **Фонові завдання**: `ADD_FLUSH_POLICY`, `ADD_TTL_CLEANUP_POLICY`,
  `ADD_SNAPSHOT_POLICY`, `DELETE_JOB`, `RUN_JOB`, `JOBS`, `JOB_STATS`.
- **Зберігання**: лише в пам'яті (`none`), наскрізний запис у логовану таблицю
  (`sync_table` — за замовчуванням) або знімки через таблицю (`SAVE`/`BGSAVE`).
- **Фоновий воркер**, що сканує `pgredis.jobs` і запускає задачі за розкладом.

## Обмеження (прочитайте перед розгортанням)

- **Сесійно-локальний простір ключів.** Кожен PostgreSQL-бекенд має власну
  копію in-memory простору ключів. Встановлення ключа в одному з'єднанні **не**
  робить його видимим в іншому — якщо тільки ви не в режимі `sync_table`
  (за замовчуванням), де інший бекенд читає трвалий рядок при першій команді і
  ліниво завантажує його в пам'ять.
- **Спільного простору ключів поки немає.** GUC `pg_redis.storage_mode = 'shared'`
  зарезервовано для майбутнього релізу. v0.1 завжди поводиться як сесійно-локальний.
- **`async_table` відкочується до `sync_table`** у v0.1. Інфраструктура є, але
  реальне асинхронне батчове скидання потребує спільного простору ключів.
- **AOF лише каркас, не реалізований.** `BGREWRITEAOF` повертає `false` та
  видає `NOTICE`. Таблиця `pgredis.aof` існує для сумісності в майбутньому.
- **Транзакції.** Тривалі рядки в `pgredis.store` беруть участь у транзакції,
  що викликає команду: `ROLLBACK` після `SET` видаляє рядок. In-memory копія
  **не** відкочується; натомість наступний доступ в тому бекенді ліниво
  перезавантажує дані з трвалої таблиці через `XactCallback`.

## Архітектура

Короткий огляд того, що де виконується, що зберігається в RAM, що на диску, і
як вони синхронізуються.

### Модель процесів

`pg_redis` — це динамічна бібліотека, що завантажується в кожен PostgreSQL-бекенд
(процес, який обслуговує SQL-з'єднання). Кожна SQL-функція в `pgredis."…"` є
C-функцією з цієї бібліотеки і виконується **всередині** процесу бекенда — без
сокету, без рівня протоколу. PostgreSQL працює за моделлю «один процес на
з'єднання», тому:

- Кожен бекенд має **свою** копію in-memory простору ключів.
- У v0.1 спільного простору між бекендами немає; видимість між сесіями
  забезпечується через трвалу таблицю (див. «Рівень зберігання» нижче).
- Один необов'язковий **фоновий воркер** (`pg_redis bgworker`) реєструється при
  `shared_preload_libraries = 'pg_redis'`. Він виконується як окремий процес,
  тікає кожні `pg_redis.flush_interval` секунд і відправляє задачі з
  `pgredis.jobs`.

### In-memory простір ключів (на бекенд)

Гаряча гілка знаходиться у приватній ділянці адресного простору бекенда і
ніколи не звертається до диска:

```text
TopMemoryContext
└── PgRedisMemoryContext           ← довгоживучий, значення palloc'уються тут
    └── HTAB "pg_redis_store"      ← dynahash, ключ (text) → PgRedisEntry
        └── PgRedisEntry           ← для одного Redis-ключа
            ├── key[1025]          ← inline NUL-terminated, фіксований розмір
            ├── type               ← STRING | INT | HASH | LIST
            ├── expire_at / has_expire  ← TTL (TimestampTz)
            ├── dirty / deleted / version / memory_usage
            └── value (union)
                ├── string_value   → char *  (palloc'd, NUL-terminated)
                ├── int_value      → int64   (inline, без alloc)
                ├── hash_value     → PgRedisHash { HTAB полів → PgRedisHashField, count, mem }
                └── list_value     → PgRedisList { doubly-linked PgRedisListNode head/tail, length, mem }
```

Ключові структури даних (див. [src/types.h](src/types.h)):

| Структура | Призначення | Де виділяється |
| --- | --- | --- |
| `PgRedisEntry` | Один Redis-ключ | Слот `dynahash` всередині `PgRedisMemoryContext` |
| `PgRedisHash` | Контейнер хешу зі своїм `HTAB` полів | `PgRedisMemoryContext` |
| `PgRedisHashField` | Одне поле хешу (`field[1025]` inline + `char *value`) | `PgRedisMemoryContext` |
| `PgRedisList` + `PgRedisListNode` | Двозв'язний список (LPUSH/RPUSH з обох кінців) | `PgRedisMemoryContext` |

Примітки щодо in-memory дизайну:

- Верхньорівневе сховище — це PostgreSQL-`dynahash` HTAB (`utils/hsearch.h`),
  прив'язаний до `PgRedisMemoryContext` (див. [src/kv_store.c](src/kv_store.c)).
  Усі алокації для значень (рядки, вузли списків, поля хешів) ідуть в той самий
  контекст, тому `FLUSHALL` реалізований як один `MemoryContextReset` + відбудова HTAB.
- Значення хешів використовують **вкладений** HTAB для кожного Redis-ключа хешу —
  `O(1)` HGET/HSET незалежно від кількості полів.
- Списки — це інтрузивні двозв'язні вузли з кешованими `head`, `tail` і
  `length`, що дає `O(1)` LPUSH/RPUSH/LPOP/RPOP/LLEN.
- TTL зберігається як `TimestampTz` безпосередньо на записі; закінчення терміну
  **ліниве** — перевіряється при кожному доступі через `pg_redis_store_lookup()` —
  плюс необов'язкове регулярне сканування через задачу `ttl_cleanup`.
- `PgRedisEntry.dirty`, `.deleted`, `.version` існують для (майбутнього) шляху
  асинхронного скидання; в режимі sync запис зберігається до повернення функції
  і `dirty` одразу очищається.

### Рівень зберігання (тривале — в таблицях PostgreSQL)

Що зберігається й де, все під схемою `pgredis`
(див. [pg_redis--1.1.sql](pg_redis--1.1.sql)):

| Таблиця | Містить | Записується |
| --- | --- | --- |
| `pgredis.store` | Один рядок на Redis-ключ: `(key, type, value bytea, expire_at, version, updated_at)` плюс **віртуальний generated column** PG18 `key_bytes`. `value` містить рядок/ціле число у TLV-кодуванні, або `NULL` для батьківських хешів/списків | пакетним скиданням до коміту в режимі `sync_table` через SPI |
| `pgredis.hash_fields` | Один рядок на поле хешу: `(key, field, value bytea)`. FK `key → pgredis.store(key) ON DELETE CASCADE` | upsert/delete per-field при скиданні |
| `pgredis.list_items` | Один рядок на елемент списку: `(key, ord bigint, value bytea)`. `ord` — стабільний монотонний порядковий номер | insert/delete per-element при скиданні |
| `pgredis.meta` | Рядки метаданих у вільній формі (jsonb), зарезервовано | внутрішні потреби розширення |
| `pgredis.jobs` | Таблиця планувальника завдань | `ADD_*_POLICY`, `DELETE_JOB`, bgworker / `RUN_JOB` |
| `pgredis.job_stats` | Лічильники на задачу | bgworker та `RUN_JOB` |
| `pgredis.snapshots` | Точковий дамп простору ключів як `jsonb`, первинний ключ `uuidv7()` | `SAVE` / `BGSAVE` |
| `pgredis.aof` | Каркас append-only журналу. Не заповнюється в v0.1 | зарезервовано |

Записи ніколи не потрапляють на диск синхронно на кожну команду. Мутатори
(`SET`/`INCR`/`HSET`/`LPUSH`/…) позначають запис у per-backend **dirty-set**
і ставлять у чергу томбстони для `DEL` / TTL-виселення в список
`pending_deletes`. При `XACT_EVENT_PRE_COMMIT` транзакції користувача dirty-set
спустошується в **одній SPI-сесії** через шість кешованих планів `SPI_keepplan`,
параметризованих масивами через `unnest()`, щоб весь батч — рядки store,
поля hash, елементи list — скинувся максимум за шість викликів
`SPI_execute_plan` (див. [src/persistence.c](src/persistence.c) `run_flush`).

### Інтеграція з транзакціями

Оскільки все виконується всередині бекенда, тривалі записи використовують
**транзакцію, що викликає команду**:

- `SET k v` → оновлює in-memory запис, потім робить upsert у `pgredis.store`
  через SPI в тій самій `XID`. `ROLLBACK` видалить рядок.
- `XactCallback` (`pg_redis_xact_cb_persistence`) слухає `XACT_EVENT_ABORT` і
  встановлює per-backend прапор «потрібне перезавантаження». **Наступний** доступ
  ліниво перезавантажує трвалий вигляд; до того часу in-memory копія ще показує
  відкотне значення — виконайте один GET після rollback для ресинхронізації.
- Ліниве завантаження запускається також при першій команді у свіжому бекенді:
  сканується `pgredis.store WHERE expire_at IS NULL OR expire_at > now()` і
  заповнюється in-memory HTAB.

### Фоновий воркер

Реєструється лише при `shared_preload_libraries = 'pg_redis'` **та**
`pg_redis.enable_background_worker = on` (див. [src/bgworker.c](src/bgworker.c)).
Цикл:

1. При старті постмайстра `_PG_init` викликає `pg_redis_bgworker_register()`.
2. Воркер підключається до бази `postgres` і входить у цикл: чекає на своєму
   latch протягом `pg_redis.flush_interval` секунд, потім викликає
   `pg_redis_jobs_tick()`.
3. `_tick` відкриває транзакцію, сканує `pgredis.jobs WHERE enabled AND
   next_run <= now()`, обробляє кожен `job_type` (`ttl_cleanup`,
   `snapshot_save`, `flush_dirty_keys`), оновлює `last_run` / `next_run` /
   `last_error` і лічильники в `pgredis.job_stats`.
4. `SIGTERM` → чисте завершення. `SIGHUP` → перечитування GUC.

### Приклад: `SET` від початку до кінця (режим sync_table)

1. `pgredis."SET"(k, v)` входить у `pg_redis_set` в [src/pg_redis.c](src/pg_redis.c).
2. Перевіряються аргументи (`pg_redis_check_key_len`, `pg_redis_check_value_len`).
3. `pg_redis_persistence_load_if_needed()` — перший виклик у цьому бекенді
   читає `pgredis.store` + `pgredis.hash_fields` + `pgredis.list_items`.
4. `pg_redis_store_upsert(k)` — знаходить або створює запис у HTAB.
5. Старе in-memory значення звільняється; нове palloc'ується в
   `PgRedisMemoryContext`; `version++`.
6. `pg_redis_mark_dirty(e)` — ідемпотентне додавання в dirty-set. SPI поки немає.
7. Функція повертає `true` у SQL. При `XACT_EVENT_PRE_COMMIT` dirty-set
   спустошується; тривалі рядки потрапляють у **поточну** транзакцію. `ROLLBACK`
   пропускає скидання і відкидає dirty-set.

## Збірка

Вимоги: **PostgreSQL 18+ (жорстка вимога)**, C-тулчейн, `pg_config` у `PATH`
та заголовки сервера PostgreSQL (наприклад, `postgresql-server-dev-18` на
Debian/Ubuntu, `postgresql@18` у Homebrew). Збірка відмовиться компілюватися
проти більш ранніх версій — pg_redis використовує функції тільки PG18
(`uuidv7()`, virtual generated columns) у своєму SQL.

```bash
make
sudo make install
```

На macOS з Homebrew:

```bash
PATH="/opt/homebrew/opt/postgresql@18/bin:$PATH" make
sudo PATH="/opt/homebrew/opt/postgresql@18/bin:$PATH" make install
```

### Функції PostgreSQL 18, що використовуються

- **`uuidv7()`** для `pgredis.snapshots.snapshot_id` — часовпорядковані
  ідентифікатори без sequence і без затримки round-trip.
- **Virtual generated columns** — `pgredis.store.key_bytes` обчислюється при
  читанні як `octet_length(key)`; не займає місця на диску і ніколи не застаріває.
- **Compile-time gate** — `src/pg_redis.c` генерує `#error` при компіляції
  проти PG молодшого за 18.

## Docker

Надається багатоетапний `Dockerfile`, що збирає розширення проти
`postgres:<PG_VERSION>-bookworm`, запускає `make installcheck` всередині образу
та створює runtime-образ із встановленим розширенням.

```bash
# Зібрати runtime-образ (postgres + pg_redis передвстановлено)
make docker-build                # PG_VERSION=18 за замовчуванням
PG_VERSION=18 make docker-build  # явно

# Запустити регресійні тести всередині Docker
make docker-test

# Запустити postgres із попередньо завантаженим розширенням
make docker-up
psql "postgres://postgres:postgres@localhost:5432/postgres" -c \
    "CREATE EXTENSION pg_redis; SELECT pgredis.\"INFO\"();"
make docker-down
```

`make docker-test` успішний лише якщо кожен регресійний файл у `test/sql/`
збігається з `test/expected/`. У разі відмови виводяться `test/regression.diffs`
та лог postgres.

Тести охоплюють всю SQL-поверхню:

| Файл | Покриває |
| --- | --- |
| `basic.sql` | SET/GET/DEL/EXISTS, INCR/DECR (включаючи помилку для нечислових), MEMORY_USAGE, FLUSHALL, KEYS |
| `ttl.sql` | Семантика EXPIRE/TTL (`-1`/`-2`), ліниве закінчення, `EXPIRE 0` / від'ємні як delete |
| `hashes.sql` | HSET/HGET/HDEL/HEXISTS + WRONGTYPE в обидва боки |
| `lists.sql` | LPUSH/RPUSH/LPOP/RPOP/LLEN, NULL при пустому pop, порядок LIFO, WRONGTYPE |
| `persistence.sql` | Наскрізний запис sync_table, SAVE + знімки, семантика ROLLBACK |
| `admin.sql` | STATS, INFO, BGSAVE, BGREWRITEAOF, virtual column PG18 на `pgredis.store`, UUIDv7 snapshot ids |
| `jobs.sql` | ADD_TTL_CLEANUP_POLICY / ADD_FLUSH_POLICY / ADD_SNAPSHOT_POLICY, JOBS, RUN_JOB, JOB_STATS, DELETE_JOB |

Для регенерації очікуваного виводу:

```bash
make docker-regen
```

## CI

GitHub Actions запускає два паралельні джоби при кожному push до `main` та
при кожному pull request:

| Джоб | Команда | Що перевіряє |
| --- | --- | --- |
| `regression-pg18` | `make docker-test PG_VERSION=18` | Повний `pg_regress` suite (basic, ttl, hashes, lists, persistence, flush, admin, jobs, async_table) |
| `async-pg18` | `make docker-test-async PG_VERSION=18` | `test/async/*.sql` — сценарії зі `shared_preload_libraries=pg_redis` та `storage_mode=shared`, які `pg_regress` не може покрити |

При падінні джобу відповідний артефакт (`regression-results-pg18` або
`async-results-pg18`) завантажується в GitHub Actions і містить `test/results/`,
`regression.diffs` або лог кластера для локального дебагу.

## Бенчмарк vs Redis

Python-харнес у `bench/` виконує однакові навантаження проти реального Redis і
pg_redis, потім виводить порівняльний звіт. Навантаження охоплює рядки
(`SET`/`GET`/`EXISTS`/`DEL`), лічильники (`INCR`/`DECR`), хеші
(`HSET`/`HGET`/`HDEL`) і списки (`LPUSH`/`RPUSH`/`LPOP`/`RPOP`/`LLEN`).

```bash
# За замовчуванням: 1000 ітерацій на операцію
make docker-bench

# Більша вибірка (повільніше)
BENCH_N=5000 make docker-bench
```

Приклад виводу (один клієнт, одне з'єднання, BENCH_N=500 на Docker Desktop /
Apple Silicon):

```text
Op       System        ops/sec        p50        p95        p99       winner
SET      redis         21.1k/s     44.2us     75.1us     95.3us    5.4x redis
         pg_redis       3.9k/s    247.5us    309.7us    354.6us
GET      redis         18.1k/s     52.2us     69.9us     89.2us       2.1x pg
         pg_redis      37.3k/s     25.3us     49.3us     50.8us
...
Aggregate throughput: redis = 18.7k/s, pg_redis = 4.3k/s  (redis 4.3x faster overall)
```

pg_redis програє на записах (кожен SET проходить через SQL-парсер, планувальник
і INSERT у `pgredis.store`), але виграє на чистих читаннях, бо пошук у хешмапі
відбувається внутрішньопроцесно, тоді як redis-py потребує round-trip по TCP.

## Встановлення в базу даних

```sql
CREATE EXTENSION pg_redis;
```

Розширення створює схему `pgredis` та вісім внутрішніх таблиць (`store`,
`hash_fields`, `list_items`, `meta`, `jobs`, `job_stats`, `snapshots`, `aof`).
Усі таблиці зареєстровані як extension config tables, тому `pg_dump` включає
дані користувача.

Для видалення:

```sql
DROP EXTENSION pg_redis CASCADE;
```

### Міграція v0.1 → v0.2 (pg_redis 1.0 → 1.1) — НЕСУМІСНА ЗМІНА

v1.1 змінює формат на диску:

- `pgredis.store.value` тепер `bytea` (раніше `jsonb`), з бінарним TLV
  `[u8 tag][u32 length_le][payload]` для рядків/цілих.
- Поля хешів — у `pgredis.hash_fields(key, field, value bytea)`.
- Елементи списків — у `pgredis.list_items(key, ord bigint, value bytea)`.

Автоматичної міграції немає: рядки v1.0 у форматі `jsonb` неможливо перевести
у TLV без C-контексту кодування. Скрипт оновлення відмовляється запускатися
при непорожньому просторі ключів.

**Необхідний workflow перед оновленням:**

```sql
-- Варіант A: дані можна викинути
SELECT pgredis."FLUSHALL"();

-- Варіант B: зробити знімок, потім очистити
SELECT pgredis."SAVE"();
SELECT pgredis."FLUSHALL"();

ALTER EXTENSION pg_redis UPDATE TO '1.1';
```

Якщо простір ключів непорожній, оновлення видасть `RAISE` з інструкцією
виконати `FLUSHALL` спершу.

**Відкат:** потребує
`DROP EXTENSION pg_redis; CREATE EXTENSION pg_redis VERSION '1.0';`.
Дані не повертаються у формат `jsonb` схеми v1.0.

## Чому всі функції в лапках ПРОПИСНИМИ?

PostgreSQL приводить ненаведені в лапках ідентифікатори до нижнього регістру.
Команди Redis канонічно ПРОПИСНІ, тому, щоб `pgredis."SET"` і `pgredis."GET"`
відповідали документації один до одного, розширення оголошує їх як **наведені в
лапках** ідентифікатори. Це означає, що ви теж маєте брати їх у лапки при
виклику:

```sql
-- Працює
SELECT pgredis."SET"('user:1:name', 'Alice');

-- Помилка: function pgredis.set(unknown, unknown) does not exist
SELECT pgredis.set('user:1:name', 'Alice');
```

Функції `pgredis.set(text, text)` не існує; тільки `pgredis."SET"(text, text)`.

## Приклади SQL

```sql
CREATE EXTENSION pg_redis;

-- Рядки
SELECT pgredis."SET"('user:1:name', 'Alice');
SELECT pgredis."GET"('user:1:name');
SELECT pgredis."DEL"('user:1:name');

-- Лічильники
SELECT pgredis."INCR"('hits');
SELECT pgredis."INCR"('hits');
SELECT pgredis."GET"('hits');                 -- '2'

-- TTL (-2 = ключ відсутній, -1 = без TTL, інакше — секунди)
SELECT pgredis."SET"('session:abc', 'token');
SELECT pgredis."EXPIRE"('session:abc', 60);
SELECT pgredis."TTL"('session:abc');          -- 59..60

-- Хеші
SELECT pgredis."HSET"('user:1', 'name', 'Alice');
SELECT pgredis."HSET"('user:1', 'age', '30');
SELECT pgredis."HGET"('user:1', 'name');
SELECT pgredis."HEXISTS"('user:1', 'age');

-- Списки (LPUSH на початок, RPUSH на кінець)
SELECT pgredis."RPUSH"('queue', 'job1');
SELECT pgredis."RPUSH"('queue', 'job2');
SELECT pgredis."LPOP"('queue');               -- 'job1'

-- Адміністрування
SELECT * FROM pgredis."KEYS"();
SELECT pgredis."MEMORY_USAGE"();
SELECT pgredis."INFO"();
SELECT * FROM pgredis."STATS"();

-- Знімок у таблицю snapshots
SELECT pgredis."SAVE"();

-- Планування фонових завдань
SELECT pgredis."ADD_TTL_CLEANUP_POLICY"('30 seconds');
SELECT pgredis."ADD_SNAPSHOT_POLICY"('1 hour');
SELECT * FROM pgredis."JOBS"();
SELECT pgredis."RUN_JOB"(1);                  -- виконати задачу inline
SELECT * FROM pgredis."JOB_STATS"();
```

## Конфігурація (GUC)

| GUC | Тип | За замовчуванням | Ефект |
| --- | --- | --- | --- |
| `pg_redis.persistence_mode` | string | `sync_table` | `none`, `sync_table`, `async_table`, `snapshot` або `aof`. `aof` відкочується до `sync_table`. `async_table` функціонує зі `storage_mode=shared`; інакше понижується до `sync_table` з `WARNING`. |
| `pg_redis.storage_mode` | string | `session` | `session` (per-backend HTAB) або `shared` (кластерний HTAB, видимий кожному бекенду). `shared` потребує `shared_preload_libraries='pg_redis'`. |
| `pg_redis.flush_interval` | int, секунди | `5` | Інтервал тіку фонового воркера. В режимі `async_table` — верхня межа вікна довговічності. |
| `pg_redis.flush_batch_size` | int | `1000` | Макс. dirty-записів за тік (зарезервовано). |
| `pg_redis.ttl_cleanup_interval` | int, секунди | `30` | Стандартний інтервал TTL-зачистки. |
| `pg_redis.max_key_size` | int, байти | `1024` | Відхиляє ключі довші за це значення. |
| `pg_redis.max_value_size` | int, байти | `1048576` | Відхиляє значення довші за це значення. |
| `pg_redis.enable_background_worker` | bool | `off` | Вмикає воркер (потребує `shared_preload_libraries`). |
| `pg_redis.shared_max_memory` | int, МБ | `256` | Обмеження DSA-сегменту для змінно-розмірних навантажень у `storage_mode=shared`. Тільки для постмайстра. |
| `pg_redis.dirty_ring_size` | int, слоти | `65536` | Слоти в спільному dirty-ring для `async_table`. Тільки для постмайстра. |
| `pg_redis.lock_partitions` | int | `16` | Кількість секційних LWLock для спільного HTAB. Тільки для постмайстра. |
| `pg_redis.async_full_action` | enum | `block` | Що робить продюсер при повному dirty-ring: `block` або `sync_flush`. |
| `pg_redis.bgworker_database` | string | `postgres` | База даних, до якої під'єднується фоновий воркер (для SPI). Якщо вона не існує, воркер записує один `FATAL` і **не перезапускається** (`BGW_NEVER_RESTART`) замість циклічного рестарту. Тільки для постмайстра, `SUPERUSER_ONLY`. |
| `pg_redis.ring_reclaim_tick_interval` | int, тіки | `1024` | Кожні N тіків дренажу воркер виконує прохід відновлення слотів dirty-ring, що «застрягли» у стані `WRITING` (продюсер обірвався між резервуванням і публікацією). |
| `pg_redis.ring_slot_stuck_timeout` | int, мс | `5000` | Скільки слот може лишатися у стані `WRITING`, перш ніж прохід відновлення поверне його в `EMPTY`. |

## Режими зберігання

### `session` (за замовчуванням)

Кожен бекенд має власний `MemoryContext` і `dynahash` HTAB. Записи йдуть у
in-memory копію і — в режимі `sync_table` — у `pgredis.store` в поточній
транзакції. Інші бекенди бачать ваші записи, читаючи трвалий рядок при
наступному ліниному завантаженні.

### `shared`

Простір ключів живе в спільній пам'яті PostgreSQL (`ShmemInitHash`,
`HASH_STRINGS`) і видимий кожному бекенду. Змінно-розмірні навантаження
(рядки, карти полів хешів, ланцюги елементів списків) живуть у єдиному
DSA-сегменті розміром `pg_redis.shared_max_memory`. Доступ серіалізується
**виключно** `pg_redis.lock_partitions` зовнішніми секційними `LWLock`
(у v1.2 прибрано `HASH_PARTITION`: його внутрішня функція секціонування
`string_hash` не збігалася із зовнішньою `hash_bytes`, що пошкоджувало HTAB
під конкуренцією). HTAB наперед розрахований на `max_entries`, тож після старту
не змінює розмір.

Потребує `shared_preload_libraries='pg_redis'`.

> **Апгрейд до 1.2 (НЕСУМІСНА ЗМІНА розкладки спільної пам'яті).** Прибирання
> `HASH_PARTITION` змінює in-shmem розкладку HTAB. Спільна пам'ять
> відбудовується з трвалих таблиць при старті постмайстра, тож після
> встановлення бінарника 1.2 потрібен **чистий перезапуск кластера** (або
> `DROP EXTENSION pg_redis; CREATE EXTENSION pg_redis;`). Трвалі таблиці та
> бінарний TLV-формат **не змінюються** — міграція даних не потрібна.

## Режими зберігання (persistence)

| Режим | Що відбувається при записі | Довговічний після краш? | Видимий іншим бекендам? |
| --- | --- | --- | --- |
| `none` | лише пам'ять; скидання no-op | ні | ні |
| `sync_table` (за замовч.) | пам'ять + dirty-set; скидання при `XACT_EVENT_PRE_COMMIT` | так (відкочується з транзакцією) | так (після першого ліниного завантаження) |
| `async_table` | пам'ять + `PgRedisDirtyEvent` у dirty-ring; команда повертається до довговічності | так, з обмеженим вікном краш-втрат | так, одразу (спільний HTAB авторитетний) |
| `snapshot` | `SAVE`/`BGSAVE` → рядки в `pgredis.snapshots` | за знімком | знімки глобальні |
| `aof` | лише каркас; `BGREWRITEAOF` повертає false | ні | н/д |

### Конфігурація для `async_table`

Мінімальний `postgresql.conf` для асинхронного шляху:

```conf
shared_preload_libraries = 'pg_redis'
pg_redis.storage_mode = 'shared'
pg_redis.persistence_mode = 'async_table'
pg_redis.enable_background_worker = on
pg_redis.shared_max_memory = 256MB
pg_redis.dirty_ring_size = 65536
pg_redis.lock_partitions = 16
pg_redis.async_full_action = 'block'
pg_redis.flush_interval = 5
```

Кеш-навантаження (допустимі втрати, низька затримка):

```conf
pg_redis.flush_interval = 10
pg_redis.async_full_action = 'block'
```

Сховище сесій (мінімальні втрати, висока пропускна здатність):

```conf
pg_redis.flush_interval = 1
pg_redis.async_full_action = 'sync_flush'
synchronous_commit = on
```

Найшвидший режим кешу:

```conf
synchronous_commit = off
pg_redis.flush_interval = 30
```

## Фонові воркери

При налаштуванні:

```conf
shared_preload_libraries = 'pg_redis'
pg_redis.enable_background_worker = on
```

…і рестарті PostgreSQL розширення реєструє один фоновий воркер («pg_redis
bgworker»), що підключається до бази `postgres`. Кожен тік виконує:

1. **Планувальник завдань**: запускає будь-яку задачу з `pgredis.jobs`, де
   `enabled = true` і `next_run <= now()`.
2. **Скидання dirty-ring** (лише при `persistence_mode=async_table`): якщо
   кількість подій ≥ `dirty_ring_size / 4` АБО час від останнього скидання ≥
   `flush_interval`, воркер відкриває транзакцію і скидає ring у таблиці.

Ви завжди можете запустити задачу inline:
`SELECT pgredis."RUN_JOB"(<id>)`.

## Обмеження транзакцій

- Трвалий рядок у `pgredis.store` бере участь у транзакції, що викликає.
  `ROLLBACK` видаляє рядок.
- In-memory копія **не** відкочується. При `XACT_EVENT_ABORT` розширення
  позначає «потрібне перезавантаження», і наступний доступ ліниво завантажить
  відкотений трвалий вигляд з таблиці.
- **Наслідок**: між відкоченим `SET` і наступним `GET` у тому ж бекенді
  in-memory значення залишається новим. Виконайте GET після rollback для
  ресинхронізації.

## Модель паралелізму

PostgreSQL — один процес на з'єднання. v0.1 зберігає простір ключів у
**приватній пам'яті бекенда**, тому він не є спільним. Коректність між сесіями
забезпечується через `pgredis.store`:

1. Бекенд A робить `SET k v` → пам'ять + рядок таблиці в транзакції A.
2. Бекенд B робить `GET k` → перший доступ у B ліниво завантажує з таблиці →
   B отримує `v`.

При `pg_redis.persistence_mode = 'none'` крок 2 поверне `NULL`.

## Дорожня карта

- Відтворення append-only файлу (`BGREWRITEAOF`, AOF replay).
- Транзакційно-усвідомлений in-memory rollback.
- `SUBSCRIBE`/`PUBLISH`, відсортовані множини, потоки — довгострокові плани.
- Розподіл за базою даних / простором імен.

## Примітки до релізів

### v1.2 — `async_table` + `storage_mode=shared` (без змін схеми на диску)

- `pg_redis.storage_mode='shared'` тепер функціональний. Простір ключів живе
  в спільній пам'яті PostgreSQL, підтримуваній DSA-сегментом. Видимість між
  бекендами — миттєва.
- `pg_redis.persistence_mode='async_table'` тепер функціональний. Мутуючі
  команди публікують `PgRedisDirtyEvent` у спільний dirty-ring; BGW скидає
  його в трвалі таблиці.
- Чотири нові GUC: `pg_redis.shared_max_memory`, `pg_redis.dirty_ring_size`,
  `pg_redis.lock_partitions`, `pg_redis.async_full_action`.
- Неправильна конфігурація (`async_table` + `session`) видає `WARNING` і
  відкочується до поведінки `sync_table`.
- `ROLLBACK` транзакції з `async_table`-мутаціями видає `WARNING` з кількістю
  вже підтверджених подій, які NOT будуть скасовані.
- Немає змін схеми на диску; схема v1.1 достатня.

## Ліцензія

Дивіться `LICENSE` (Apache-2.0).
