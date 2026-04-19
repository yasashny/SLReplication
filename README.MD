## Архитектура

Система поддерживает три режима работы:
- **Single-Leader** (ДЗ №2): один лидер, запись только через него
- **Multi-Leader** (ДЗ №3): несколько лидеров, запись через любого из них, LWW-разрешение конфликтов
- **Leaderless** (ДЗ №4): нет лидера, любой узел — координатор, quorum read/write

```
┌─────────────────────────────────────────────────────────────────┐
│                         CLI / Controller                         │
│  - Управление кластером (add/remove nodes, set mode)             │
│  - Настройка quorum (W, R, strict/sloppy)                        │
│  - Hinted handoff, anti-entropy, wipe                            │
│  - Бенчмарки                                                     │
└─────────────────────────────────────────────────────────────────┘
                              │ TCP (JSON Lines)
     ┌────────┬────────┬──────┼──────┬────────┬────────┐
     ▼        ▼        ▼      ▼      ▼        ▼        ▼
  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐
  │  H1  │ │  H2  │ │  H3  │ │  H4  │ │  H5  │ │  S1  │ │  S2  │
  │ HOME │ │ HOME │ │ HOME │ │ HOME │ │ HOME │ │SPARE │ │SPARE │
  └──────┘ └──────┘ └──────┘ └──────┘ └──────┘ └──────┘ └──────┘
       ↕         ↕         ↕         ↕         ↕
       Quorum read/write (W ACKs для записи, R ответов для чтения)
```

## Режим Leaderless (ДЗ №4)

### Фиксированная модель кластера
- **7 узлов**: 5 home-реплик + 2 spare-узла
- Все ключи реплицируются на одни и те же 5 home-реплик
- Spare-узлы используются только для hints при sloppy quorum

### Координатор запроса
- Любой узел может принять `PUT` или `GET`
- Узел, получивший запрос, становится координатором
- Координатор рассылает запросы home-репликам и собирает ответы

### Quorum-модель
- **N = 5** (фиксировано)
- **W** и **R** настраиваются через CLI, обязательно `W + R > 5`
- **Strict quorum**: ACKs считаются только от home-реплик
- **Sloppy quorum** (только для записи): если home-реплика недоступна, ACK можно получить от spare-узла

### Write Path
1. Координатор генерирует `version = (lamport++, nodeId)` и `operationId`
2. Отправляет `REPL_WRITE` на все 5 home-реплик
3. Ждёт W подтверждений (strict: только от home, sloppy: home + spare)
4. При sloppy: spare-узлы сохраняют hints вместо обычных записей

### Read Path + Async Read-Repair
1. Координатор отправляет `READ_QUERY` на все 5 home-реплик
2. Ждёт R ответов, выбирает запись с максимальной версией
3. Возвращает результат клиенту
4. Асинхронно отправляет repair stale-репликам

### Hinted Handoff
- Spare-узлы хранят `HintRecord` с `intendedHomeNodeId`
- Команда `runHintedHandoff` доставляет hints на восстановленные home-реплики
- После успешной доставки hint удаляется

### Merkle Tree и Anti-Entropy
- **16 фиксированных бакетов**, `bucket = abs(hash(key)) mod 16`
- Leaf hash: `SHA-256(sorted records: key|value|lamport|nodeId|operationId)`
- Внутренние узлы: `SHA-256(leftHash + rightHash)`
- `showMerkleRoot <nodeId>` — текущий root hash
- `runAntiEntropyCluster` — cluster-wide anti-entropy pass:
  1. Собирает Merkle root со всех home-реплик
  2. Находит пары с различающимися root
  3. Для каждой пары: Merkle diff → обмен записями из различающихся бакетов

### Восстановление после wipe
- `wipeNodeData <nodeId>` очищает store, hints, deduplicator
- `runAntiEntropyCluster` восстанавливает wiped home-реплику из остальных

## Предыдущие режимы

### mode=single (ДЗ №2)
- Один лидер, запись только через него
- Follower возвращает `NOT_LEADER`
- Режимы репликации: async, sync, semi-sync

### mode=multi (ДЗ №3)
- Несколько лидеров, запись через любого
- LWW конфликт-резолюшн через Lamport Clock
- Топологии: mesh, ring, star

## Версионирование (Lamport Clock)

Каждый узел имеет `lamportCounter: long`.

При локальном PUT: `L := L + 1`, `version := (L, nodeId)`

При получении удалённой версии: `L := max(L, remoteLamport) + 1`

Сравнение: `(l1, n1) > (l2, n2)` если `l1 > l2` или `(l1 == l2 и n1 > n2)`

## Сетевой протокол

### Транспорт
- TCP сокеты, JSON Lines (одно JSON сообщение на строку + `\n`)

### Типы сообщений (Leaderless)
| Тип | Направление | Описание |
|-----|-------------|----------|
| `CLIENT_PUT` | CLI → Node | Запись ключа (координатор) |
| `CLIENT_GET` | CLI → Node | Чтение ключа (координатор) |
| `REPL_WRITE` | Node → Node | Quorum-запись на home/spare |
| `REPL_WRITE_ACK` | Node → Node | ACK quorum-записи |
| `READ_QUERY` | Node → Node | Quorum-чтение |
| `READ_RESPONSE` | Node → Node | Ответ quorum-чтения |
| `HINTED_HANDOFF_TRANSFER` | Spare → Home | Доставка hint |
| `MERKLE_*` | CLI ↔ Node | Anti-entropy протокол |
| `WIPE_DATA` | CLI → Node | Очистка данных |

## Сборка

```bash
./gradlew build
./gradlew nodeJar cliJar
```

## Запуск

### Запуск 7 узлов (leaderless)
```bash
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar H1 5001
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar H2 5002
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar H3 5003
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar H4 5004
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar H5 5005
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar S1 5006
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar S2 5007
```

### Настройка leaderless-кластера в CLI
```bash
java -jar build/libs/kv-cli-1.0-SNAPSHOT.jar
> addNode H1 localhost 5001
> addNode H2 localhost 5002
> addNode H3 localhost 5003
> addNode H4 localhost 5004
> addNode H5 localhost 5005
> addNode S1 localhost 5006
> addNode S2 localhost 5007
> setMode leaderless
> setHomeReplicas H1 H2 H3 H4 H5
> setSpareNodes S1 S2
> setQuorum 3 3
```

## Команды CLI

### Управление кластером
```
addNode <nodeId> <host> <port>
removeNode <nodeId>
listNodes
setMode single|multi|leaderless
status
setReplicationDelayMs <min> <max>
```

### Leaderless-режим
```
setHomeReplicas <id1>..<id5>        # Назначить 5 home-реплик
setSpareNodes <id1> <id2>           # Назначить 2 spare-узла
setQuorum <W> <R>                   # W+R > 5
setWriteQuorumMode strict|sloppy    # Режим записи
dumpHints [--target <nodeId>]       # Показать hints
runHintedHandoff [--target <nodeId>]# Доставить hints
showMerkleRoot <nodeId>             # Merkle root hash
wipeNodeData <nodeId>               # Очистить данные узла
runAntiEntropyCluster               # Cluster-wide anti-entropy
```

### Пользовательские команды
```
put <key> <value> [--target <nodeId>]
get <key> [--target <nodeId>]
dump [--target <nodeId>]
getAll <key>
clusterDump
```

### Бенчмарки
```
benchmark --run-all-leaderless      # 9 leaderless-сценариев
benchmark --run-all                 # Single-leader сценарии
benchmark --run-all-multi           # Multi-leader сценарии
benchmark --threads 16 --ops 100 --put-ratio 0.5
```

## Структура проекта

```
src/main/kotlin/com/yasashny/slreplication/
├── common/
│   ├── model/
│   │   └── Messages.kt              # Модели, протокол, LeaderlessPayload
│   └── network/
│       ├── TcpServer.kt             # TCP сервер
│       └── TcpClient.kt             # TCP клиент, пул соединений
├── node/
│   ├── Node.kt                      # Узел: single/multi/leaderless
│   ├── NodeMain.kt                  # Точка входа Node
│   ├── LamportClock.kt              # Lamport clock
│   ├── storage/
│   │   ├── KeyValueStore.kt         # Хранилище с версионированием
│   │   └── HintStore.kt             # Хранилище hints (spare-узлы)
│   ├── leaderless/
│   │   ├── LeaderlessCoordinator.kt # Quorum write/read, repair, handoff
│   │   └── MerkleTree.kt            # Merkle Tree (16 бакетов, SHA-256)
│   └── replication/
│       ├── ReplicationManager.kt    # Репликация: single + multi
│       ├── OperationDeduplicator.kt # Дедупликация с TTL
│       └── TopologyRouter.kt        # Топологии mesh/ring/star
└── cli/
    ├── CliMain.kt                   # Интерактивный CLI
    ├── ClusterManager.kt            # Управление кластером + anti-entropy
    └── Benchmark.kt                 # Бенчмарки single + multi + leaderless

benchmarks/
├── results.csv                      # Single-leader результаты
├── results-multi.csv                # Multi-leader результаты
└── results-leaderless.csv           # Leaderless результаты

demo-scenario.md                     # Демонстрационные сценарии
report.md                            # Отчёт по бенчмаркам
```
