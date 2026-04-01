# Демонстрационные сценарии

## Часть 1 — Single-Leader (ДЗ №2)

### Инициализация кластера (single-leader)

```
addNode A localhost 5001
addNode B localhost 5002
addNode C localhost 5003
setLeader A
listNodes
```

### Сценарий 1 — Follower отклоняет запись (NOT_LEADER)

```
put hello world --target B
# Error: NOT_LEADER - This node is not the leader
# Leader: A
```

### Сценарий 2 — Async lag + stale read

```
setReplication async
setRF 3
setReplicationDelayMs 2000 3000

put color red
get color --target B
# (nil) или старое значение

# Ждём ~3 секунды
get color --target B
# color = red

setReplicationDelayMs 0 0
```

### Сценарий 3 — Sync + RF

```
setReplication sync
setRF 3

put x 1
# OK

# Останавливаем C (Ctrl+C)
put x 2
# Error: NOT_ENOUGH_REPLICAS

setRF 2
put x 3
# OK

# Останавливаем B (Ctrl+C)
put x 4
# Error: NOT_ENOUGH_REPLICAS

setRF 1
put x 5
# OK
```

### Сценарий 4 — Semi-sync

```
setReplication semi-sync
setRF 3
setSemiSyncAcks 1
setReplicationDelayMs 2000 4000

put score 42
# OK (быстро, после 1 ACK)

get score --target C
# Возможно: (nil)

# Ждём ~4 секунды
get score --target C
# score = 42

setReplicationDelayMs 0 0
```

---

## Часть 2 — Multi-Leader (ДЗ №3)

### Подготовка кластера (10 узлов)

Запустить 10 узлов в отдельных терминалах:

```bash
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar L1 5001
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar L2 5002
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar L3 5003
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar L4 5004
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar F1 5005
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar F2 5006
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar F3 5007
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar F4 5008
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar F5 5009
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar F6 5010
```

CLI:
```
addNode L1 localhost 5001
addNode L2 localhost 5002
addNode L3 localhost 5003
addNode L4 localhost 5004
addNode F1 localhost 5005
addNode F2 localhost 5006
addNode F3 localhost 5007
addNode F4 localhost 5008
addNode F5 localhost 5009
addNode F6 localhost 5010
setMode multi
setLeaders L1 L2 L3 L4
setTopology mesh
status
```

---

### Сценарий 1 — Конфликт на одном ключе

Конкурентные PUT одного ключа на двух разных leader-узлах, расхождение, затем LWW-схождение.

```
setReplicationDelayMs 2000 3000

# Записываем один ключ на двух разных лидерах
put x valueFromL1 --target L1
put x valueFromL2 --target L2

# Сразу проверяем — видим расхождение
getAll x

# Ждём 5 секунд пока репликация завершится

# Проверяем снова — все узлы сошлись к одному значению
getAll x

setReplicationDelayMs 0 0
```

**Ожидаемый результат:**
- Сразу: на L1 `valueFromL1`, на L2 `valueFromL2`, остальные — nil или одно из значений
- После схождения: все узлы имеют одно значение — то, у которого выше `(lamport, nodeId)` по LWW
- Версии видны в выводе `getAll` в формате `[v=(lamport, nodeId)]`

---

### Сценарий 2 — Replication lag и stale read

```
setReplicationDelayMs 3000 5000

put mykey fresh_value --target L1

# Сразу читаем с follower
get mykey --target F1
# (nil) — данные ещё не дошли

get mykey --target L1
# mykey = fresh_value — на лидере уже есть

# Ждём 6 секунд

get mykey --target F1
# mykey = fresh_value — данные дошли

getAll mykey

setReplicationDelayMs 0 0
```

---

### Сценарий 3 — Mesh vs Ring

#### 3a. Mesh — быстрое схождение

```
setTopology mesh
setReplicationDelayMs 500 1000

put speed mesh_value --target L1

# Через ~2 секунды
getAll speed
# Все узлы имеют значение — mesh рассылает всем напрямую
```

#### 3b. Ring — медленное схождение

```
setTopology ring
setReplicationDelayMs 500 1000

put speed ring_value --target L1

# Через ~2 секунды — только часть узлов получила
getAll speed

# Через ~10 секунд — все узлы получили (10 хопов × ~0.75с)
getAll speed

setReplicationDelayMs 0 0
```

**Ожидаемый результат:**
- Mesh: ~1-2 сек до полного схождения (одновременная рассылка)
- Ring: ~7-10 сек (последовательные хопы по кольцу)

---

### Сценарий 4 — Star как bottleneck / single point of failure

```
setTopology star
setStarCenter F1
setReplicationDelayMs 0 0

# Штатная работа
put star_test value1 --target L1
# Ждём 1 сек
getAll star_test
# Все узлы имеют value1

# Останавливаем центральный узел F1 (Ctrl+C в терминале F1)

put star_test2 value2 --target L2
# Ждём 2 сек
getAll star_test2
# Данные только на L2 — остальные не получили (центр недоступен)
```

**Ожидаемый результат:**
- В штатном режиме: star корректно работает через центральный узел
- При падении центра: данные застревают на лидере-источнике

---

### Сценарий 5 — Ring ломается при падении узла без removeNode

```
setTopology ring
setReplicationDelayMs 0 0

# Нормальная работа
put ring_ok value1 --target L1
# Ждём 3 сек
getAll ring_ok
# Все узлы имеют value1

# Останавливаем F3 (Ctrl+C) БЕЗ вызова removeNode
# Кольцо разорвано: ... -> F2 -> [F3 мёртв] -> F4 -> ...

put ring_broken value2 --target L1
# Ждём 5 сек
getAll ring_broken
# Узлы после F3 в кольце НЕ получили значение

# Обновляем membership — кольцо перестраивается
removeNode F3

put ring_fixed value3 --target L1
# Ждём 3 сек
getAll ring_fixed
# Все живые узлы получили value3

# Follower отклоняет запись в multi mode
put test value --target F1
# Error: NOT_LEADER_FOR_WRITE
```
