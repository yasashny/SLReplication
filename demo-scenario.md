## Инициализация кластера (общая для всех сценариев)

```
addNode A localhost 5001
addNode B localhost 5002
addNode C localhost 5003
setLeader A
listNodes
```

---

## Сценарий 1 — Follower отклоняет запись (NOT_LEADER)

```
# Текущий лидер — A. Пробуем писать напрямую на follower B.
put hello world --target B
# Ожидаемый результат:
# Error: NOT_LEADER - This node is not the leader
# Leader: A
```

---

## Сценарий 2 — Async lag + stale read

```
setReplication async
setRF 3
setReplicationDelayMs 2000 3000

# Записываем через лидера
put color red

# Сразу читаем с follower — может вернуть (nil) или старое значение
get color --target B

# Ждём ~3 секунды, пока репликация дойдёт
get color --target B
# Ожидаемый результат: color = red

# Убираем задержку
setReplicationDelayMs 0 0
```

---

## Сценарий 3 — Sync + RF: поведение при выключении follower'ов

```
setReplication sync
setRF 3

# Запись при 3 живых узлах — успех
put x 1
# OK

# Останавливаем follower C (Ctrl+C в терминале 3)
# RF=3, но живых follower'ов только 1 → не хватает 2 подтверждений
put x 2
# Error: NOT_ENOUGH_REPLICAS

# Понижаем RF=2 — теперь достаточно 1 follower'а
setRF 2
put x 3
# OK

# Останавливаем follower B (Ctrl+C в терминале 2)
# RF=2, follower'ов 0 → нет подтверждений
put x 4
# Error: NOT_ENOUGH_REPLICAS

# RF=1 — лидер отвечает без ожидания ACK
setRF 1
put x 5
# OK
```

---

## Сценарий 4 — Semi-sync: быстрый OK, догоняем в фоне

```
# Перезапускаем узлы B и C если были остановлены, заново добавляем в кластер
# (или продолжаем если живы)

setReplication semi-sync
setRF 3
setSemiSyncAcks 1
setReplicationDelayMs 2000 4000

# Записываем — лидер ждёт только 1 ACK (от ближайшего follower'а)
put score 42
# OK — приходит быстро (после первого ACK)

# Сразу читаем с отстающего follower'а — может быть stale
get score --target C
# Возможно: (nil) или старое значение

# Ждём ~4 секунды — фоновая репликация догоняет до RF=3
get score --target C
# score = 42

dump --target B
dump --target C
# Оба должны показать score = 42

# Убираем задержку
setReplicationDelayMs 0 0
```
