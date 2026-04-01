## Архитектура

Система поддерживает два режима работы:
- **Single-Leader** (ДЗ №2): один лидер, запись только через него
- **Multi-Leader** (ДЗ №3): несколько лидеров, запись через любого из них, LWW-разрешение конфликтов

```
┌─────────────────────────────────────────────────────────────────┐
│                         CLI / Controller                         │
│  - Управление кластером (add/remove nodes, set leader/mode)     │
│  - Настройка репликации (mode, topology, RF, K, delay)          │
│  - Пользовательские команды (put/get/dump/getAll/clusterDump)   │
│  - Бенчмарки                                                    │
└─────────────────────────────────────────────────────────────────┘
                              │ TCP (JSON Lines)
     ┌────────┬────────┬──────┼──────┬────────┬────────┐
     ▼        ▼        ▼      ▼      ▼        ▼        ▼  ...
  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐
  │  L1  │ │  L2  │ │  L3  │ │  L4  │ │  F1  │ │  F2  │  ...
  │LEADER│ │LEADER│ │LEADER│ │LEADER│ │FOLLOW│ │FOLLOW│
  └──────┘ └──────┘ └──────┘ └──────┘ └──────┘ └──────┘
       ↕         ↕         ↕         ↕
       Репликация по топологии (mesh/ring/star)
```

### Node (Узел хранилища)
- In-memory хранилище `Map<String, String>` с версионированием (Lamport clock)
- TCP сервер для приёма запросов
- Поддержка ролей: Leader и Follower
- Репликация по топологии (mesh, ring, star)
- LWW (Last Write Wins) разрешение конфликтов
- Очередь pending операций с ретраями
- Дедупликация операций по `operationId` с TTL 5 минут

### CLI (Cluster Controller)
- Управление составом кластера и режимами
- Назначение лидеров и топологий
- Выполнение пользовательских команд
- Отладочные команды (getAll, clusterDump)
- Запуск бенчмарков

## Режимы работы

### mode=single (ДЗ №2)
- Один лидер, запись только через него
- Follower возвращает `NOT_LEADER`
- Режимы репликации: async, sync, semi-sync
- Replication Factor (RF), Semi-sync ACKs (K)

### mode=multi (ДЗ №3)
- Несколько лидеров (4 leader + 6 follower в стандартной конфигурации)
- Follower возвращает `NOT_LEADER_FOR_WRITE` с `leaderNodeId`
- Версионирование через Lamport Clock
- LWW конфликт-резолюшн
- Топологии: mesh, ring, star
- Eventual consistency

## Lamport Clock

Каждый узел имеет локальный счётчик `lamportCounter: long`.

**При локальном PUT на leader:**
```
L := L + 1
version := (L, currentNodeId)
```

**При получении REPL_PUT с версией (remoteLamport, remoteNodeId):**
```
L := max(L, remoteLamport) + 1
```

В хранилище сохраняется **пришедшая версия**, а не новая локальная.

## Разрешение конфликтов (LWW)

Сравнение версий `(lamport, nodeId)`:
```
(l1, n1) > (l2, n2) если l1 > l2 ИЛИ (l1 == l2 И n1 > n2)
```

При применении входящей версии:
- Если ключа нет — сохранить
- Если `incoming.version > local.version` — заменить
- Иначе — проигнорировать

## Топологии репликации

### Mesh
Лидер после PUT рассылает `REPL_PUT` **всем остальным узлам** напрямую. Другие узлы не форвардят.
- Быстрое схождение (1 хоп)
- Большой сетевой трафик

### Ring
Узлы образуют кольцо (порядок по сортировке `nodeId`). Каждый узел знает своего `next` и пересылает обновление только ему. Дедупликация предотвращает бесконечное хождение.
- Медленное схождение (N-1 хопов)
- Минимальный трафик
- Чувствителен к разрыву (падение узла без removeNode)

### Star
Центральный узел (`setStarCenter <nodeId>`) — хаб для всех сообщений.
- Нецентральный лидер → отправляет в центр → центр рассылает всем
- Центральный лидер → рассылает всем напрямую
- Single point of failure при падении центра

## Семантика записи на Follower

**Single mode:** follower возвращает `NOT_LEADER` с указанием лидера.

**Multi mode:** follower возвращает `NOT_LEADER_FOR_WRITE` с `leaderNodeId`.

## Сетевой протокол

### Транспорт
- TCP сокеты, JSON Lines (одно JSON сообщение на строку + `\n`)

### Типы сообщений
| Тип | Направление | Описание |
|-----|-------------|----------|
| `CLIENT_PUT` | CLI → Node | Запись ключа |
| `CLIENT_GET` | CLI → Node | Чтение ключа |
| `CLIENT_DUMP` | CLI → Node | Дамп всех данных |
| `RESPONSE` | Node → CLI | Ответ на запрос |
| `CLUSTER_UPDATE` | CLI → Node | Обновление конфигурации |
| `REPL_PUT` | Node → Node | Репликация записи |
| `REPL_ACK` | Node → Node | Подтверждение репликации |

### REPL_PUT (multi mode)
```json
{
  "type": "REPL_PUT",
  "operationId": "uuid",
  "originNodeId": "L1",
  "sourceNodeId": "L1",
  "key": "x",
  "value": "hello",
  "version": { "lamport": 5, "nodeId": "L1" }
}
```

### REPL_ACK
```json
{
  "type": "REPL_ACK",
  "operationId": "uuid",
  "fromNodeId": "F1"
}
```

### Коды ошибок
| Код | Описание |
|-----|----------|
| `NOT_LEADER` | Запись на follower в single mode |
| `NOT_LEADER_FOR_WRITE` | Запись на follower в multi mode |
| `NOT_ENOUGH_REPLICAS` | Не хватает ACK (sync/semi-sync) |
| `TIMEOUT` | Таймаут операции |
| `BAD_REQUEST` | Некорректный запрос |
| `INVALID_MODE` | Некорректный режим |
| `INVALID_TOPOLOGY` | Некорректная топология |

## Идемпотентность

Каждая операция репликации имеет уникальный `operationId`. Узлы хранят `seenOpIds` с TTL 5 минут:
- Если `opId` уже встречался — операция не применяется повторно и не форвардится
- ACK отправляется повторно
- Периодическая очистка устаревших `opId`

## Задержка репликации

`setReplicationDelayMs <min> <max>` добавляет случайную задержку **перед отправкой** репликационного сообщения на стороне отправителя.

## Сборка

```bash
./gradlew build
./gradlew nodeJar cliJar
```

## Запуск

### Запуск узлов
```bash
# Через JAR
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar L1 5001
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar L2 5002
# ... и т.д.

# Через Gradle
./gradlew runNode --args="L1 5001"
```

### Запуск CLI
```bash
java -jar build/libs/kv-cli-1.0-SNAPSHOT.jar
# или
./gradlew runCli
```

## Команды CLI

### Управление кластером
```
addNode <nodeId> <host> <port>      # Добавить узел
removeNode <nodeId>                 # Удалить узел
listNodes                           # Список узлов
setLeader <nodeId>                  # Назначить лидера (single mode)
setMode single|multi                # Режим работы
setTopology mesh|ring|star          # Топология (multi mode)
setStarCenter <nodeId>              # Центр звезды (star topology)
setLeaders <id1> [id2] ...         # Назначить лидеров (multi mode)
setReplication async|semi-sync|sync # Режим репликации (single mode)
setRF <int>                         # Replication Factor (single mode)
setSemiSyncAcks <int>               # K для semi-sync
setReplicationDelayMs <min> <max>   # Задержка репликации
status                              # Статус кластера
```

### Пользовательские команды
```
put <key> <value> [--target <nodeId>]  # Записать
get <key> [--target <nodeId>]          # Прочитать
dump [--target <nodeId>]               # Дамп данных узла
```

### Отладочные команды
```
getAll <key>                        # GET на все узлы (value + version)
clusterDump                         # DUMP на все узлы
```

### Бенчмарки
```
benchmark --threads 16 --ops 100 --put-ratio 0.5
benchmark --run-all                 # Single-leader сценарии (18 прогонов)
benchmark --run-all-multi           # Multi-leader сценарии (12 прогонов)
benchmark --key-space 5             # Маленький keySpace для конфликтов
benchmark --help
```

## Структура проекта

```
src/main/kotlin/com/yasashny/slreplication/
├── common/
│   ├── model/
│   │   └── Messages.kt              # Модели, протокол, Version, LWW
│   └── network/
│       ├── TcpServer.kt             # TCP сервер
│       └── TcpClient.kt             # TCP клиент, пул соединений
├── node/
│   ├── Node.kt                      # Узел: single/multi, Lamport clock, LWW
│   ├── NodeMain.kt                  # Точка входа Node
│   ├── storage/
│   │   └── KeyValueStore.kt         # Хранилище с версионированием
│   └── replication/
│       ├── ReplicationManager.kt    # Репликация: single + multi + топологии
│       └── OperationDeduplicator.kt # Дедупликация с TTL
└── cli/
    ├── CliMain.kt                   # Интерактивный CLI
    ├── ClusterManager.kt            # Управление кластером
    └── Benchmark.kt                 # Бенчмарки single + multi

benchmarks/
├── results.csv                      # Single-leader результаты
└── results-multi.csv                # Multi-leader результаты

demo-scenario.md                     # Демонстрационные сценарии
report.md                            # Отчёт по бенчмаркам
```
