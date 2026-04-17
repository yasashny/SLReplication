# Отчёт по бенчмаркам: Leaderless-репликация

## Конфигурация

- **Кластер:** 7 узлов — 5 home-реплик (H1–H5) + 2 spare-узла (S1, S2), localhost.
- **N = 5** (фиксированное число home-реплик).
- **Потоки:** 16, **Операций/поток:** 100 (1600 ops per run), warmup 50 ops, keyspace 10000.
- **Replication delay:** 5–25 ms в Части A/B (инжектирован на `REPL_WRITE`, чтобы сделать видимыми трейд-оффы W/R). В Части C delay=0, чтобы измерить чистый recovery.
- **Скрипт графиков:** `benchmarks/charts/generate.py` — воспроизводимо собирает 4 PNG из CSV.

---

## Часть A — Сравнение quorum-конфигураций (strict, 6 прогонов, delay 5–25 ms)

| # | W | R | Put Ratio | Throughput (ops/sec) | Avg (ms) | p95 (ms) | PUT avg (ms) | PUT p95 (ms) | GET avg (ms) | GET p95 (ms) | Success |
|---|---|---|-----------|---------------------:|---------:|---------:|-------------:|-------------:|-------------:|-------------:|---------|
| 1 | 3 | 3 | 0.8 |  933.49 | 15.45 | 26.00 | 17.98 | 26.00 |  6.13 |  8.00 | 1600/1600 |
| 2 | 3 | 3 | 0.2 | 1624.37 |  8.43 | 22.00 | 18.21 | 27.00 |  5.96 |  7.00 | 1600/1600 |
| 3 | 4 | 2 | 0.8 |  813.42 | 18.31 | 27.00 | 21.39 | 27.00 |  5.58 |  6.00 | 1600/1600 |
| 4 | 4 | 2 | 0.2 | 1598.40 |  9.11 | 26.00 | 21.89 | 28.00 |  5.75 |  7.00 | 1600/1600 |
| 5 | 5 | 1 | 0.8 |  717.17 | 20.22 | 30.00 | 25.14 | 31.00 |  1.90 |  6.00 | 1600/1600 |
| 6 | 5 | 1 | 0.2 | 1834.86 |  7.12 | 28.00 | 25.52 | 29.00 |  1.91 |  6.00 | 1600/1600 |

![Chart 1: Throughput by Quorum Configuration](benchmarks/charts/chart1_throughput_quorum.png)

![Chart 2: PUT vs GET p95](benchmarks/charts/chart2_latency_put_get.png)

### Анализ

**PUT latency растёт монотонно с ростом W.** При случайной задержке Uniform(5, 25) ms на REPL_WRITE координатор ждёт «наи-W-меньшее» время из 4 удалённых сэмплов (+1 локальный home). Ожидаемая picture: W=3 → быстрейшие 2 из 4, W=4 → 3 из 4, W=5 → все 4. Замеры подтверждают: PUT p95 идёт **26 → 27 → 31 ms**, PUT avg **18 → 21 → 25 ms**. Это прямо trade-off кворумной записи — больше W = больше latency, меньше `NOT_ENOUGH_REPLICAS` рисков при отказах.

**GET latency резко падает при R=1.** При R=3 координатор шлёт READ_QUERY всем 5 home-репликам и ждёт 3 ответа (включая свой). На localhost это ~6 ms. При R=1 — координатор читает локально (он же home-реплика) без сетевых вызовов → GET avg **1.9 ms, p50 1 ms**. Это x3 ускорение на read-heavy трафике.

**Throughput — compound-эффект.** На write-heavy (80% PUT) доминирует PUT latency, throughput падает **933 → 813 → 717 ops/sec** (−23%) при увеличении W. На read-heavy (20% PUT) доминирует GET, и R=1 даёт **1835 ops/sec** против **1624** при R=3 (+13%). Ключевой выбор: если нагрузка перекос-read — увеличивать R в ущерб W не стоит, лучше R=1/W=5. Если перекос-write — W=3/R=3 оптимальнее по балансу.

---

## Часть B — Recovery wiped home-реплики (1 прогон, delay=0)

| # | Scenario | Keys Written | Recovered | Recovery Time (ms) |
|---|----------|-------------:|----------:|-------------------:|
| 9 | RECOVERY | 1000 | 1000 | 78 |

![Chart 4: Recovery](benchmarks/charts/chart4_recovery.png)

### Анализ

Чистый baseline: перед записью все 5 home-реплик обнуляются (`wipeNodeData` каждой), затем заливается ровно **1000** ключей (`recovery_key_1..1000`). Ключ `H1` очищается, `runAntiEntropyCluster` запускается.

**Результат:** все 1000 ключей восстановлены за **78 ms**, `antiEntropyRecoveredKeys = 1000` ровно (delta от wipe'а). Сверка идёт по **Merkle-based diff** — эффективно, хотя в этом частном случае все 16 бакетов различаются, потому что store wiped-узла пуст.

**Механизм (см. `AntiEntropyOrchestrator.kt`):**
1. `MERKLE_ROOT_REQUEST` ко всем 5 home → wiped H1 возвращает root пустого store (константа `SHA-256("")`-based), остальные — общий ненулевой root.
2. Пары `(H1, H2), (H1, H3), (H1, H4), (H1, H5)` попадают в divergent (4 пары).
3. Для каждой пары — `MERKLE_DIFF_REQUEST` без `diffBuckets`: возвращаются 16 leaf-хэшей. У H1 все 16 = `SHA-256("")`, у живых — ненулевые. Diff = все 16 бакетов.
4. Второй `MERKLE_DIFF_REQUEST` с `diffBuckets = [0..15]` — каждая сторона отдаёт записи из этих бакетов. `mergeByNewestVersion` мерджит по Lamport.
5. `MERKLE_RECORDS_TRANSFER` заливает недостающее в H1 — записи проходят через `putVersioned` (LWW).

**Почему read-repair не заменяет anti-entropy:**
- Read-repair (`ReadCoordinator.scheduleAsyncReadRepair`) срабатывает только при `GET` на конкретный ключ. После `wipeNodeData H1` H1 не теряет роль home-реплики — он по-прежнему получает координируемые GET'ы. Но его `KeyValueStore.get` возвращает `null` / stale, координатор это видит и шлёт repair. **Однако** repair срабатывает только для читаемых ключей. 1000 неиспользуемых ключей никто бы не прочитал → они остались бы потерянными на H1 навсегда.
- Anti-entropy сверяет **весь store**, независимо от паттерна чтения. Именно это и требуется для полного recovery за один pass.

---

## Общие выводы

1. **Quorum trade-off (W, R).** При реальной сетевой задержке больше W = дороже запись (PUT avg 18 → 25 ms при W 3 → 5); меньше R = дешевле чтение (GET avg 6 → 2 ms при R 3 → 1). Не существует «лучшего» значения W/R — выбор зависит от read/write-смеси нагрузки. При 80% writes `(3, 3)` выдаёт +30% throughput против `(5, 1)`; при 20% writes `(5, 1)` выдаёт +13% throughput против `(3, 3)`.

2. **Sloppy quorum + hinted handoff.** В тесте по ДЗ (W=3, 1 home down, 4 живых) `hintsCreated = 0` — sloppy не активируется, потому что W набирается с home. Это корректное поведение: spare-fallback срабатывает только когда home действительно не хватает. При `W = N_живых_home` разница была бы драматической.

3. **Read-repair vs anti-entropy.** Read-repair показал `staleReadCount=2, readRepairCount=8` за Часть A — он работает, но только на тех ключах, которые читаются. Anti-entropy нужен для ключей вне горячего set'а и для полного recovery после wipe.

4. **Recovery через Merkle.** Полное восстановление 1000 ключей на пустой home-реплике заняло 78 ms — это сверка 16 leaf-хэшей + перенос записей из всех 16 бакетов. Если бы расходился 1 ключ, diff показал бы 1 различающийся бакет и перенос был бы в ~16 раз дешевле по данным. Один и тот же код чинит и мелкие расхождения, и полный wipe.

5. **Инъекция задержки критична для корректной визуализации.** До правки бенчмарк выполнялся на delay=0: все операции ~5–7 ms (dominated by TCP localhost RTT), трейд-оффы W/R не различимы. С delay=5–25 ms на `REPL_WRITE` картина встала на место — именно так ДЗ §15 предписывает использовать `setReplicationDelayMs` для воспроизведения stale reads, lag между репликами и recovery after wipe.

---

## Как воспроизвести

```bash
./gradlew cliJar nodeJar
# в 7 терминалах: java -jar build/libs/kv-node-1.0-SNAPSHOT.jar Hi 500i  (i=1..5)
#                  java -jar build/libs/kv-node-1.0-SNAPSHOT.jar Si 500(5+i)  (i=1..2)

cat <<EOF | java -jar build/libs/kv-cli-1.0-SNAPSHOT.jar
addNode H1 localhost 5001
...
addNode S2 localhost 5007
setMode leaderless
setHomeReplicas H1 H2 H3 H4 H5
setSpareNodes S1 S2
setQuorum 3 3
benchmark --run-all-leaderless
exit
EOF

python3 benchmarks/charts/generate.py
```

CSV пишется в `benchmarks/results-leaderless.csv`, графики — в `benchmarks/charts/`.
