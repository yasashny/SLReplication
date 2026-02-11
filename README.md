## Архитектура

Система состоит из двух типов процессов:

```
┌─────────────────────────────────────────────────────────────────┐
│                         CLI / Controller                         │
│  - Управление кластером (add/remove nodes, set leader)          │
│  - Настройка репликации (mode, RF, K, delay)                    │
│  - Пользовательские команды (put/get/dump)                      │
│  - Бенчмарки                                                    │
└─────────────────────────────────────────────────────────────────┘
                              │ TCP
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
   ┌────────────┐      ┌────────────┐      ┌────────────┐
   │  Node A    │      │  Node B    │      │  Node C    │
   │  (LEADER)  │─────▶│ (follower) │      │ (follower) │
   │            │─────────────────────────▶│            │
   │ Map<K,V>   │      │ Map<K,V>   │      │ Map<K,V>   │
   └────────────┘      └────────────┘      └────────────┘
         │                   │                   │
         └───────────────────┴───────────────────┘
                    Репликация (REPL_PUT → ACK)
```

### Node (Узел хранилища)
- In-memory хранилище `Map<String, String>`
- TCP сервер для приёма запросов
- Поддержка ролей: Leader и Follower
- Репликация операций на Followers
- Очередь pending операций с ретраями
- Дедупликация операций по `operationId` с TTL 5 минут

### CLI (Cluster Controller)
- Управление составом кластера (add/remove nodes)
- Назначение лидера
- Настройка параметров репликации
- Выполнение пользовательских команд (put/get/dump)
- Запуск бенчмарков

## Режимы репликации

| Режим | Поведение | Latency | Consistency |
|-------|-----------|---------|-------------|
| **Async** | Лидер отвечает сразу, репликация в фоне | Низкая | Eventual |
| **Sync** | Лидер ждёт ACK от (RF-1) followers | Высокая | Strong |
| **Semi-Sync** | Лидер ждёт K ACK, догоняет в фоне | Средняя | Partial |

### Async
- Лидер отвечает клиенту сразу после локального применения
- Репликация идёт в фоне
- Максимальная производительность, но возможны stale reads

### Sync
- Лидер ждёт ACK от (RF-1) followers
- Запись считается успешной только при получении нужного количества подтверждений
- Гарантия консистентности, но выше latency

### Semi-Sync
- Лидер ждёт ACK от K followers (где 1 ≤ K ≤ RF-1)
- После получения K ACK возвращает OK клиенту
- Продолжает догонять оставшиеся реплики в фоне
- Компромисс между Sync и Async

## Параметры

- **RF (Replication Factor)** - целевое количество узлов, на которые должна быть доставлена запись (включая лидера)
- **K (semiSyncAcks)** - количество followers, после ACK от которых возвращается OK в режиме semi-sync

### Валидация параметров
- `RF <= clusterSize` (иначе ошибка)
- При `semi-sync`: `1 <= K <= RF-1` (иначе ошибка)
- При `K = RF-1` semi-sync эквивалентен sync

## Сетевой протокол

### Транспорт
- TCP сокеты
- JSON Lines: одно JSON сообщение на строку, завершается `\n`

### Типы сообщений
| Тип | Направление | Описание |
|-----|-------------|----------|
| `CLIENT_PUT` | CLI → Node | Запись ключа |
| `CLIENT_GET` | CLI → Node | Чтение ключа |
| `CLIENT_DUMP` | CLI → Node | Дамп всех данных |
| `RESPONSE` | Node → CLI | Ответ на запрос |
| `CLUSTER_UPDATE` | CLI → Node | Обновление конфигурации |
| `REPL_PUT` | Leader → Follower | Репликация записи |
| `REPL_ACK` | Follower → Leader | Подтверждение репликации |

### Формат сообщений

**Клиентский запрос:**
```json
{
  "type": "CLIENT_PUT",
  "requestId": "uuid",
  "clientId": "client-1",
  "key": "mykey",
  "value": "myvalue"
}
```

**Успешный ответ:**
```json
{
  "type": "RESPONSE",
  "requestId": "uuid",
  "status": "OK",
  "key": "mykey",
  "value": "myvalue"
}
```

**Ошибка:**
```json
{
  "type": "RESPONSE",
  "requestId": "uuid",
  "status": "ERROR",
  "errorCode": "NOT_LEADER",
  "errorMessage": "This node is not the leader",
  "leaderNodeId": "A"
}
```

### Коды ошибок
| Код | Описание |
|-----|----------|
| `NOT_LEADER` | Запись на follower (возвращает leaderNodeId) |
| `NOT_ENOUGH_REPLICAS` | Не удалось получить достаточно ACK |
| `TIMEOUT` | Таймаут операции |
| `BAD_REQUEST` | Некорректный запрос |
| `UNKNOWN_NODE` | Неизвестный узел |

## Семантика записи на Follower

**Redirect (NOT_LEADER)** - follower возвращает ошибку с указанием лидера, клиент должен переотправить запрос на лидера.

## Идемпотентность

Каждая операция репликации имеет уникальный `operationId`. Followers хранят `seenOpIds` с TTL 5 минут:
- Если `opId` уже встречался - операция игнорируется
- ACK отправляется повторно
- Периодическая очистка устаревших `opId`

## Сборка

```bash
./gradlew build
```

## Запуск

### Быстрый старт (Windows)

1. **Запуск кластера:**
```bash
start-cluster.bat
```

2. **Запуск CLI:**
```bash
start-cli.bat
```

3. **Остановка кластера:**
```bash
stop-cluster.bat
```

### Ручной запуск

**Запуск узла:**
```bash
# Через Gradle
./gradlew runNode --args="A 5001"
./gradlew runNode --args="B 5002"
./gradlew runNode --args="C 5003"

# Или через JAR
./gradlew nodeJar cliJar
java -jar build/libs/kv-node-1.0-SNAPSHOT.jar A 5001
```

**Запуск CLI:**
```bash
./gradlew runCli
# или
java -jar build/libs/kv-cli-1.0-SNAPSHOT.jar
```

## Команды CLI

### Управление кластером
```
addNode <nodeId> <host> <port>    # Добавить узел
removeNode <nodeId>               # Удалить узел
listNodes                         # Список узлов
setLeader <nodeId>                # Назначить лидера
setReplication async|semi-sync|sync  # Режим репликации
setRF <int>                       # Replication Factor
setSemiSyncAcks <int>             # K для semi-sync
setReplicationDelayMs <min> <max> # Задержка репликации
status                            # Статус кластера
```

### Пользовательские команды
```
put <key> <value> [--target <nodeId>]  # Записать
get <key> [--target <nodeId>]          # Прочитать
dump [--target <nodeId>]               # Дамп данных
```

### Бенчмарки
```
benchmark --threads 16 --ops 100 --put-ratio 0.5
benchmark --run-all --output benchmarks/results.csv
benchmark --help
```

## Примеры использования

### Базовый сценарий
```
> addNode A localhost 5001
Node A added at localhost:5001

> addNode B localhost 5002
Node B added at localhost:5002

> addNode C localhost 5003
Node C added at localhost:5003

> setLeader A
Leader set to A

> setReplication sync
Replication mode set to SYNC

> setRF 3
Replication factor set to 3

> put mykey myvalue
OK

> get mykey --target B
mykey = myvalue

> dump
Data (1 entries):
  mykey = myvalue
```

### Демонстрация stale read (async)
```
> setReplication async
> setReplicationDelayMs 2000 3000
> put newkey newvalue
OK
> get newkey --target B
(nil)  # данные ещё не доехали
# через 2-3 секунды
> get newkey --target B
newkey = newvalue
```

## Структура проекта

```
src/main/kotlin/com/yasashny/slreplication/
├── common/
│   ├── model/
│   │   └── Messages.kt          # Модели данных, JSON протокол
│   └── network/
│       ├── TcpServer.kt         # TCP сервер
│       └── TcpClient.kt         # TCP клиент, пул соединений
├── node/
│   ├── Node.kt                  # Главный класс узла
│   ├── NodeMain.kt              # Точка входа Node
│   ├── storage/
│   │   └── KeyValueStore.kt     # In-memory хранилище
│   └── replication/
│       ├── ReplicationManager.kt      # Менеджер репликации
│       └── OperationDeduplicator.kt   # Дедупликация операций
└── cli/
    ├── CliMain.kt               # Интерактивный CLI
    ├── ClusterManager.kt        # Управление кластером
    └── Benchmark.kt             # Бенчмарки

benchmarks/
└── results.csv                  # Результаты бенчмарков

demo-scenario.md                 # Демонстрационные сценарии
report.md                        # Отчёт по бенчмаркам
```

## Бенчмарки

Результаты бенчмарков сохраняются в `benchmarks/results.csv`.

### Запуск всех сценариев
```
> benchmark --run-all --output benchmarks/results.csv
```
