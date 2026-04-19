# Демонстрационные сценарии

## Часть 1 — Leaderless-репликация (ДЗ №4)

### Подготовка: запуск 7 узлов

```bash
# В отдельных терминалах:
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar H1 5001
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar H2 5002
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar H3 5003
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar H4 5004
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar H5 5005
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar S1 5006
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar S2 5007
```

### Настройка CLI

```
addNode H1 localhost 5001
addNode H2 localhost 5002
addNode H3 localhost 5003
addNode H4 localhost 5004
addNode H5 localhost 5005
addNode S1 localhost 5006
addNode S2 localhost 5007
setMode leaderless
setHomeReplicas H1 H2 H3 H4 H5
setSpareNodes S1 S2
setQuorum 3 3
status
```

---

### Сценарий 1 — Strict quorum и stale read

Цель: показать strict write quorum, задержку, stale read, async read-repair.

```
# Ставим большую задержку, чтобы окно stale было достаточно длинным для наблюдения вручную
setReplicationDelayMs 5000 10000

# Записываем ключ.
# PUT возвращает OK, как только координатор собрал W=3 ACK.
# Оставшиеся 2 медленные home-реплики ещё "спят" в delay — значения у них пока нет.
put x hello

сырое состояние каждой home-реплики смотрим через clusterDump,
clusterDump
# У 2 из 5 home-реплик ключа x нет — они ещё в задержке.

# Клиентский GET через quorum — координатор собирает R=3 ответа,
# выбирает запись с max version и возвращает "hello".
# Заодно видит stale у 2 home-реплик и планирует async read-repair им.
get x

# Сразу после GET — read-repair уже успел применить свежую версию
# на stale-репликах (REPL_WRITE из read-repair идёт без инжектированной задержки).
# Даже если исходные медленные REPL_WRITE всё ещё в полёте — они будут
# отсеяны дедупликатором по operationId.
clusterDump
# Все 5 home-реплик: x = hello.

# Убираем задержку
setReplicationDelayMs 0 0
```

---

### Сценарий 2 — Sloppy quorum + Hinted Handoff

Цель: показать запись при недоступной home-реплике, создание hint, доставку.

```
# Переключаемся на sloppy quorum
setWriteQuorumMode sloppy

# "Ломаем" одну home-реплику (убиваем процесс H5 через Ctrl+C)

setQuorum 5 5

# Записываем — sloppy quorum использует spare-узел
put y world
# OK — получили W=3 ACKs (от home + spare)

# Проверяем hints на spare-узлах
dumpHints --target S1
dumpHints --target S2
# Один из spare содержит hint для H5: y=world

# Перезапускаем H5: java -jar build/libs/kv-node-1.0-SNAPSHOT.jar H5 5005
addNode H5 localhost 5005
setHomeReplicas H1 H2 H3 H4 H5
setSpareNodes S1 S2

# Доставляем hints
runHintedHandoff
# delivered=1, failed=0

# Hints исчезли
dumpHints --target S1
# (no hints)

# Данные на H5
get y --target H5
# y = world — доставлено через hinted handoff

# Возвращаемся на strict
setWriteQuorumMode strict
```

---

### Сценарий 3 — Cluster-wide Anti-Entropy

Цель: показать различие root hash, anti-entropy sync, convergence.

```
# Записываем часть данных без задержки — они должны сойтись на всех репликах
put a 1
put b 2
put c 3

# Включаем большую задержку и пишем ещё — медленные REPL_WRITE останутся в полёте,
# поэтому на части home-реплик ключей d и e ещё не будет в момент следующей проверки
setReplicationDelayMs 5000 10000
put d 4
put e 5

# Задержку можно убрать — уже запущенные delay-корутины это не отменит,
# но новых задержек не будет
setReplicationDelayMs 0 0

# Сразу смотрим сырой state — у части home-реплик d/e ещё нет
clusterDump

# Merkle root — могут различаться
showMerkleRoot H1
showMerkleRoot H2
showMerkleRoot H3
showMerkleRoot H4
showMerkleRoot H5

# Anti-entropy
runAntiEntropyCluster
# "Anti-entropy completed: N records recovered across M pairs"

# Root hash сошлись
showMerkleRoot H1
showMerkleRoot H2
showMerkleRoot H3
showMerkleRoot H4
showMerkleRoot H5
# Все одинаковые
```

---

### Сценарий 4 — Восстановление wiped home-реплики

Цель: показать потерю данных и восстановление через Merkle anti-entropy.

```
# Записываем данные
put key1 val1
put key2 val2
put key3 val3
put key4 val4
put key5 val5

# Проверяем Merkle root H3
showMerkleRoot H3

# Wipe H3
wipeNodeData H3
dump --target H3
# (empty)

showMerkleRoot H3
# Root hash изменился (пустой store)

# Восстановление
runAntiEntropyCluster
# "Anti-entropy completed: 5 records recovered..."

dump --target H3
# key1=val1, key2=val2, ... key5=val5

showMerkleRoot H3
# Совпадает с остальными
```

---

### Сценарий 5 — Smoke-check старых режимов

Цель: показать, что single и multi не сломаны.

```
# Single-leader
setMode single
setLeader H1
put single_key single_value
get single_key
# single_key = single_value

# Multi-leader
setMode multi
setLeaders H1 H2 H3 H4
setTopology mesh
put multi_key multi_value
getAll multi_key
# Все узлы видят значение

# Возврат в leaderless
setMode leaderless
setHomeReplicas H1 H2 H3 H4 H5
setSpareNodes S1 S2
setQuorum 3 3
status
```
