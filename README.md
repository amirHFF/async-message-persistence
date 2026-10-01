# chat-message-persistence

A small Java service that consumes chat-message records from Kafka (as raw `byte[]`) and
persists them into a pluggable storage backend (Redis today) for later archive lookup.
Built with the plain **Apache Kafka client** — no `spring-kafka` — to keep the dependency
graph and runtime memory footprint minimal.

## Why it looks like this

Every requirement in the task maps to a specific design decision:

| Requirement | How it's satisfied |
|---|---|
| Read `byte[]` off the broker | `ByteArrayDeserializer` on both key and value — no Kafka-side deserialization at all; the raw bytes are handed to `MessageCodec` ourselves. |
| No data may be missed | `enable.auto.commit=false`; offsets are committed **by hand**, only after a record is either persisted or successfully routed to the DLT. An outage in storage or in the DLT topic itself simply pauses that partition and retries — it never skips a record. |
| DLT for problem messages | `KafkaDeadLetterPublisher` sends unprocessable records, byte-for-byte, to `<topic>.DLT` with headers describing why (original topic/partition/offset, error reason, error detail, timestamp). |
| Storage in Redis, fully persisted | `RedisMessageRepository` (Jedis). Persistence-to-disk (RDB/AOF) is a **server-side** Redis setting — see "Redis persistence" below for what to configure on the Redis instance itself. |
| Open for extension, closed for modification (new DB later) | All persistence goes through the `MessageRepository` interface. `MessageRepositoryFactory` selects an implementation by config. Adding Postgres/Mongo/etc. later = one new class + one line in the factory; the consumer, codec, and DLT code never change. |
| No `spring-kafka` | Raw `kafka-clients` (`KafkaConsumer`/`KafkaProducer`) only. |
| Minimal / careful dependencies | Runtime deps are exactly: `kafka-clients`, `avro` (binary encode/decode only, no schema registry client), `jedis` (+ its own small transitive `commons-pool2`), `slf4j-api` + `slf4j-simple`. No Spring, no resilience framework — retry/backoff is ~10 lines of our own code. |
| Minimal memory usage | Single-threaded consumer, `max.poll.records` capped small, bounded fetch sizes, **no Redis connection pool** (a pool buys nothing for a single-threaded consumer), the original Kafka bytes are stored as-is instead of being re-serialized into a second in-memory representation (e.g. JSON). |
| "No need for fast retrieval" | Storage layout favors simple, append-friendly writes over read-optimized structures (see below). |

## Wire format: Apache Avro (binary)

The payload is Avro-encoded using `org.apache.avro.io.BinaryEncoder`/`BinaryDecoder`
(**not** Avro's JSON encoding, and **no** Confluent schema registry — the schema is
compiled into the jar, not fetched over the network). The schema lives in
`src/main/avro/ChatMessage.avsc`:

```
messageId       string  (UUID)
conversationId  string  (UUID)
senderId        string
receiverId      string
timestamp       long    (epoch millis)
rawPayload      bytes   (opaque - text today, may be voice/image/etc. later)
```

`avro-maven-plugin` generates `com.chatpersistence.avro.ChatMessageAvro` (a `SpecificRecord`)
from that schema at build time (`generate-sources` phase) — it is not hand-written and not
checked into the repo. `BinaryMessageCodec` maps between `ChatMessageAvro` (the wire
representation) and `ChatMessage` (the domain model the rest of the pipeline uses).

`rawPayload` is deliberately `bytes`, not `string`: it carries text today but is expected
to carry voice/image data later. Keeping it opaque means neither the schema nor the codec
need to change for that — only whatever consumes `rawPayload` downstream needs to interpret
it. When that day comes, prefer adding a `contentType` field with an Avro `default` over
changing this codec (see Avro's schema-evolution rules).

**This is the one seam you may need to revisit** if the real producer's schema differs
from `ChatMessage.avsc` (different field names/types, or an actual schema registry).
Everything else in the pipeline (consumer loop, retry/backoff, DLT, storage) depends only
on `MessageCodec`'s interface, not on Avro specifically.

## Architecture

```
Kafka topic ──poll(byte[])──▶ MessagePersistenceConsumer
                                    │
                                    ├─▶ MessageCodec.decode()  ──fails──▶ DeadLetterPublisher ──▶ Kafka DLT topic
                                    │        │ ok
                                    │        ▼
                                    ├─▶ MessageRepository.save()  ──non-transient fail──▶ DeadLetterPublisher
                                    │        │ ok                 ──transient fail──▶ retry w/ backoff, then
                                    │        ▼                                          pause partition & retry later
                                    └─▶ commitSync(offset)   (only after success or DLT above)
```

- `codec/` — `MessageCodec` interface + `BinaryMessageCodec` (Avro binary, the byte-layout seam).
- `storage/` — `MessageRepository` interface, `RedisMessageRepository`, `MessageRepositoryFactory`
  (the database seam).
- `dlt/` — `DeadLetterPublisher` interface + `KafkaDeadLetterPublisher`.
- `consumer/` — `MessagePersistenceConsumer`, the poll/decode/persist/commit loop.
- `config/` — `AppConfig`, properties-file + env-var configuration.
- `App` — wiring + graceful shutdown (SIGTERM-safe: finishes in-flight offset bookkeeping
  before the process exits).

### Redis layout

```
chatmsg:<messageId>            -> raw original bytes (exactly what was read from Kafka)
chatmsg:conv:<conversationId>  -> sorted set, score = timestamp, member = messageId
```

The value is the **original Kafka bytes**, not a re-encoded JSON document — this avoids
a second serialization step/library purely for storage, and means the archive is
byte-identical to what the producer sent. The sorted set is a cheap append-only index so a
future "browse archived messages for a conversation, oldest→newest" screen doesn't need a
`SCAN` over the whole keyspace; since fast retrieval isn't a requirement, no further
read-side optimization (caching, secondary indices, etc.) was added.

### Failure handling in detail

| Situation | Action |
|---|---|
| Payload doesn't decode (`MessageCodecException`) | Poison pill — retrying won't help. Routed straight to DLT. |
| Redis unreachable (`JedisConnectionException`) | **Transient.** Retried up to `storage.retry.max.attempts` times with exponential backoff. If still failing, the consumer does **not** commit the offset and does **not** DLT the message — it backs off and re-polls the same record later. An outage never causes data loss. |
| Redis returns some other error (`JedisException`, non-connection) | Treated as non-transient — sent to DLT (this is a data issue, not an outage, so it shouldn't block the whole partition indefinitely). |
| DLT publish itself fails | Offset is **not** committed; the whole record (including the original decode/storage failure) is retried on the next poll. |

### Adding a new database later

1. `public class PostgresMessageRepository implements MessageRepository { ... }`
2. In `MessageRepositoryFactory`, add: `case "postgres" -> new PostgresMessageRepository(config);`
3. Set `storage.type=postgres` (or `STORAGE_TYPE=postgres` env var).

No change to `MessagePersistenceConsumer`, `MessageCodec`, or the DLT publisher.

## Configuration

All keys live in `src/main/resources/application.properties` and can be overridden by an
environment variable of the same name (dots → underscores, upper-cased), e.g.
`kafka.bootstrap.servers` → `KAFKA_BOOTSTRAP_SERVERS`.

| Key | Default | Meaning |
|---|---|---|
| `kafka.bootstrap.servers` | `185.204.170.204:9092` | Kafka broker(s) |
| `kafka.topic` | `chat-messages` | Source topic |
| `kafka.consumer.group.id` | `chat-message-persistence` | Consumer group |
| `kafka.dlt.topic` | `<kafka.topic>.DLT` | Dead letter topic |
| `kafka.max.poll.records` | `50` | Caps per-poll memory |
| `storage.type` | `redis` | Storage backend selector |
| `redis.host` / `redis.port` | `185.204.170.204` / `6379` | Redis connection |
| `storage.retry.max.attempts` | `3` | Retries for transient storage errors |
| `storage.retry.initial.backoff.ms` | `200` | First retry delay (doubles each attempt, capped at 30s) |
| `infra.retry.backoff.ms` | `2000` | Pause before re-polling when storage looks fully down |

## Redis persistence (server-side)

This app writes with plain `SET`/`ZADD`. For the data to survive a Redis restart (the task's
"persist کامل" requirement), the **Redis server** needs persistence enabled, e.g. in
`redis.conf`:

```
appendonly yes
appendfsync everysec
```

(or RDB snapshotting via `save` directives) — this is a server configuration concern, not
something the client app can control.

## Build & run

```bash
mvn clean package
java -jar target/chat-message-persistence.jar
```

Or with Docker:

```bash
docker build -t chat-message-persistence .
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=185.204.170.204:9092 \
  -e REDIS_HOST=185.204.170.204 \
  -e REDIS_PORT=6379 \
  chat-message-persistence
```

> **Note on this repository's current state:** this environment's outbound network
> access does not include Maven Central, so `mvn test` / `mvn package` could not be run
> from here — the code was written and reviewed against the documented Kafka
> client / Jedis APIs, but please run `mvn clean verify` in an environment with normal
> internet access before deploying, and let me know if anything fails to compile so I
> can fix it immediately.

## Tests

`src/test/java` includes:
- `BinaryMessageCodecTest` — round-trip encode/decode (including a non-text/binary
  `rawPayload`, e.g. what a future voice/image message would look like), unicode text,
  and malformed/truncated/corrupted payload rejection (including an invalid UUID field).
- `AppConfigTest` — default values and env/system-property overrides.
- `MessageRepositoryFactoryTest` — unknown `storage.type` fails fast.
- `BackoffTest` — exponential backoff growth and cap.
