package io.ProjectZ.storage;

import io.ProjectZ.config.AppConfig;
import io.ProjectZ.model.ChatMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

import java.nio.charset.StandardCharsets;

/**
 * Redis-backed {@link MessageRepository}.
 * <p>
 * Uses a single {@link Jedis} connection rather than a connection pool: the consumer that
 * owns this repository is single-threaded (one poll-process-commit loop), so a pool would
 * only add idle connections and heap overhead (commons-pool2 bookkeeping) without any
 * throughput benefit - a deliberate choice given the "minimize memory usage" requirement.
 * The connection is transparently re-established on the next call after a connection error.
 * <p>
 * Layout in Redis:
 * <ul>
 *   <li>{@code chatmsg:<messageId>} -&gt; raw original bytes polled from Kafka (the value is
 *       never re-serialized, so the archive is byte-for-byte identical to what the producer
 *       sent).</li>
 *   <li>{@code chatmsg:conv:<conversationId>} -&gt; a sorted set, score = timestamp (epoch
 *       millis), member = messageId. This is only an index to let a future "view archived
 *       messages for a conversation" feature list message ids in order; it costs O(1)
 *       memory per message and keeps write path append-only.</li>
 * </ul>
 * Data is expected to be fully persisted (RDB/AOF) on the Redis server side; this class
 * does not (and cannot) change the server's persistence configuration.
 */
public final class RedisMessageRepository implements MessageRepository {

    private static final Logger log = LoggerFactory.getLogger(RedisMessageRepository.class);
    private static final String MESSAGE_KEY_PREFIX = "chatmsg:";
    private static final String CONVERSATION_INDEX_PREFIX = "chatmsg:conv:";

    private final String host;
    private final int port;
    private final String password;
    private final int connectTimeoutMs;
    private final int soTimeoutMs;

    private Jedis jedis;

    public RedisMessageRepository(AppConfig config) {
        this.host = config.redisHost();
        this.port = config.redisPort();
        this.password = config.redisPassword();
        this.connectTimeoutMs = config.redisConnectTimeoutMs();
        this.soTimeoutMs = config.redisSoTimeoutMs();
        connect();
    }

    private void connect() {
        closeQuietly();
        jedis = new Jedis(host, port, connectTimeoutMs, soTimeoutMs);
        if (password != null && !password.isBlank()) {
            jedis.auth(password);
        }
        log.info("Connected to Redis at {}:{}", host, port);
    }

    @Override
    public void save(ChatMessage message, byte[] rawPayload) throws StorageException {
        byte[] key = (MESSAGE_KEY_PREFIX + message.getMessageId()).getBytes(StandardCharsets.UTF_8);
        byte[] indexKey = (CONVERSATION_INDEX_PREFIX + message.getConversationId())
                .getBytes(StandardCharsets.UTF_8);
        byte[] member = message.getMessageId().toString().getBytes(StandardCharsets.UTF_8);

        try {
            jedis.set(key, rawPayload);
            jedis.zadd(indexKey, (double) message.getTimestampEpochMillis(), member);
        } catch (JedisConnectionException e) {
            // Infrastructure problem: caller should retry / back off, not DLT the message.
            attemptReconnect();
            throw new StorageException("Redis connection error while saving " + message.getMessageId(), e, true);
        } catch (JedisException e) {
            throw new StorageException("Redis error while saving " + message.getMessageId(), e, false);
        }
    }

    @Override
    public boolean isHealthy() {
        try {
            return "PONG".equalsIgnoreCase(jedis.ping());
        } catch (JedisException e) {
            return false;
        }
    }

    private void attemptReconnect() {
        try {
            connect();
        } catch (JedisException reconnectFailure) {
            log.warn("Redis reconnect attempt failed: {}", reconnectFailure.getMessage());
        }
    }

    private void closeQuietly() {
        if (jedis != null) {
            try {
                jedis.close();
            } catch (Exception ignored) {
                // best effort
            }
        }
    }

    @Override
    public void close() {
        closeQuietly();
    }
}
