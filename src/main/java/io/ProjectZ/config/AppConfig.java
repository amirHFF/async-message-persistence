package io.ProjectZ.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Loads configuration from {@code application.properties} on the classpath, with every
 * key overridable by an environment variable of the same name (dots replaced by
 * underscores, upper-cased) - e.g. {@code kafka.bootstrap.servers} -> {@code KAFKA_BOOTSTRAP_SERVERS}.
 * This makes the app easy to configure in Docker/K8s without editing files.
 */
public final class AppConfig {

    private final Properties properties = new Properties();

    public AppConfig() {
        this("application.properties");
    }

    AppConfig(String resourceName) {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream(resourceName)) {
            if (in != null) {
                properties.load(in);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load " + resourceName, e);
        }
    }

    public String get(String key, String defaultValue) {
        String envKey = key.replace('.', '_').toUpperCase();
        String fromEnv = System.getenv(envKey);
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv;
        }
        String fromSystemProps = System.getProperty(key);
        if (fromSystemProps != null && !fromSystemProps.isBlank()) {
            return fromSystemProps;
        }
        return properties.getProperty(key, defaultValue);
    }

    public int getInt(String key, int defaultValue) {
        return Integer.parseInt(get(key, String.valueOf(defaultValue)));
    }

    public long getLong(String key, long defaultValue) {
        return Long.parseLong(get(key, String.valueOf(defaultValue)));
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        return Boolean.parseBoolean(get(key, String.valueOf(defaultValue)));
    }

    // ---- Kafka ----
    public String kafkaBootstrapServers() {
        return get("kafka.bootstrap.servers", "185.204.170.204:9092");
    }

    public String kafkaTopic() {
        return get("kafka.topic", "message-archive");
    }

    public String kafkaConsumerGroupId() {
        return get("kafka.consumer.group.id", "chat-message-persistence");
    }

    public String kafkaDltTopic() {
        return get("kafka.dlt.topic", kafkaTopic() + ".DLT");
    }

    public int kafkaMaxPollRecords() {
        // Small on purpose: bounds per-poll memory; this pipeline does not need throughput.
        return getInt("kafka.max.poll.records", 50);
    }

    public long kafkaPollTimeoutMs() {
        return getLong("kafka.poll.timeout.ms", 1000L);
    }

    // ---- Storage ----
    public String storageType() {
        return get("storage.type", "redis");
    }

    public String redisHost() {
        return get("redis.host", "185.204.170.204");
    }

    public int redisPort() {
        return getInt("redis.port", 6379);
    }

    public String redisPassword() {
        return get("redis.password", null);
    }

    public int redisConnectTimeoutMs() {
        return getInt("redis.connect.timeout.ms", 3000);
    }

    public int redisSoTimeoutMs() {
        return getInt("redis.so.timeout.ms", 3000);
    }

    // ---- Retry / resilience ----
    public int storageMaxRetries() {
        return getInt("storage.retry.max.attempts", 3);
    }

    public long storageRetryInitialBackoffMs() {
        return getLong("storage.retry.initial.backoff.ms", 200L);
    }

    public long infraBackoffMs() {
        // Sleep applied when the whole storage backend looks down, before re-polling
        // the *same* records again (offset is not advanced).
        return getLong("infra.retry.backoff.ms", 2000L);
    }
}
