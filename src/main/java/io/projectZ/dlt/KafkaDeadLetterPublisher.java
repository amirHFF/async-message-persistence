package io.projectZ.dlt;

import io.projectZ.config.AppConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class KafkaDeadLetterPublisher implements DeadLetterPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaDeadLetterPublisher.class);

    private final Producer<String, byte[]> producer;
    private final String dltTopic;
    private final long sendTimeoutMs;

    public KafkaDeadLetterPublisher(AppConfig config) {
        this.dltTopic = config.kafkaDltTopic();
        this.sendTimeoutMs = 10_000L;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // Wait for the leader's ack (durability) but do not need every ISR replica for a DLT record.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        // Small buffers: this producer only ever carries occasional poison-pill messages.
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16_384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);

        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public boolean publish(String originalTopic, int originalPartition, long originalOffset,
                            String originalKey, byte[] originalValue, String reason, String errorDetail) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(dltTopic, originalKey, originalValue);
        record.headers()
                .add(new RecordHeader("x-original-topic", originalTopic.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("x-original-partition", String.valueOf(originalPartition).getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("x-original-offset", String.valueOf(originalOffset).getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("x-error-reason", reason.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("x-error-detail", truncate(errorDetail, 2000).getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("x-failed-at", Instant.now().toString().getBytes(StandardCharsets.UTF_8)));

        try {
            producer.send(record).get(sendTimeoutMs, TimeUnit.MILLISECONDS);
            log.warn("Routed record {}-{}@{} to DLT topic {} (reason={})",
                    originalTopic, originalPartition, originalOffset, dltTopic, reason);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while publishing to DLT", e);
            return false;
        } catch (ExecutionException | TimeoutException e) {
            log.error("Failed to publish poison-pill record {}-{}@{} to DLT",
                    originalTopic, originalPartition, originalOffset, e);
            return false;
        }
    }

    private static String truncate(String s, int max) {
        if (s == null) {
            return "";
        }
        return s.length() <= max ? s : s.substring(0, max);
    }

    @Override
    public void close() {
        producer.close();
    }
}
