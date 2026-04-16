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
# Устанавливаем задержку репликации
setReplicationDelayMs 500 1500

# Записываем ключ
put x hello
# OK — запись прошла через quorum (W=3)

# Сразу читаем — часть реплик может быть stale
get x
# Должно вернуть "hello" (координатор выбирает max version из R ответов)

# Проверяем все узлы
getAll x
# Некоторые home-реплики могут показать (nil) из-за задержки

# Ждём ~2с и проверяем снова — read-repair исправляет stale
getAll x
# Все home-реплики: x = hello

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
removeNode H5

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
# Записываем данные с задержкой
put a 1
put b 2
put c 3
setReplicationDelayMs 2000 3000
put d 4
put e 5
setReplicationDelayMs 0 0

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
