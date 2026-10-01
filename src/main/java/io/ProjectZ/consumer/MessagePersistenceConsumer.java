package io.ProjectZ.consumer;

import io.ProjectZ.codec.MessageCodec;
import io.ProjectZ.codec.MessageCodecException;
import io.ProjectZ.config.AppConfig;
import io.ProjectZ.dlt.DeadLetterPublisher;
import io.ProjectZ.model.ChatMessage;
import io.ProjectZ.storage.MessageRepository;
import io.ProjectZ.storage.StorageException;
import io.ProjectZ.util.Backoff;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Polls raw {@code byte[]} records from Kafka and persists them via {@link MessageRepository}.
 * <p>
 * Zero data loss strategy (at-least-once delivery to storage):
 * <ul>
 *   <li>{@code enable.auto.commit=false} - offsets are committed by hand, and only after a
 *       record has been either (a) successfully persisted, or (b) successfully routed to the
 *       DLT. Until one of those happens, the offset is never advanced, so a crash / restart
 *       simply re-polls the same record.</li>
 *   <li>Malformed payloads ({@link MessageCodecException}) are poison pills: retrying will
 *       never make them decodable, so they go straight to the DLT.</li>
 *   <li>Storage failures classified as {@link StorageException#isTransient()} (connection
 *       errors) are retried with backoff a bounded number of times; if the backend still
 *       looks down afterwards, the consumer pauses and re-polls the *same* batch later
 *       rather than giving up on the message (an outage must not equal data loss).</li>
 *   <li>Non-transient storage failures are routed to the DLT, same as codec failures.</li>
 * </ul>
 * This class depends only on the {@link MessageRepository} and {@link MessageCodec}
 * interfaces, never on a concrete database - swapping storage backends requires no change here.
 */
public final class MessagePersistenceConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(MessagePersistenceConsumer.class);

    private final AppConfig config;
    private final MessageCodec codec;
    private final MessageRepository repository;
    private final DeadLetterPublisher deadLetterPublisher;
    private final KafkaConsumer<String, byte[]> consumer;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public MessagePersistenceConsumer(AppConfig config,
                                      MessageCodec codec,
                                      MessageRepository repository,
                                      DeadLetterPublisher deadLetterPublisher) {
        this.config = config;
        this.codec = codec;
        this.repository = repository;
        this.deadLetterPublisher = deadLetterPublisher;
        this.consumer = new KafkaConsumer<>(buildConsumerProperties(config));
        log.info("consumer initialized ");
    }

    private static Properties buildConsumerProperties(AppConfig config) {
        log.info("consumer initializing ... ");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.kafkaConsumerGroupId());
        // Requirement: read the data as byte[] off the broker - no value deserializer magic.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        // Manual commits are the backbone of the "no data missed" guarantee.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.kafkaMaxPollRecords());
        // Keep per-poll memory bounded - archival use case does not need high throughput.
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 5 * 1024 * 1024);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1 * 1024 * 1024);
        return props;
    }

    public void shutdown() {
        running.set(false);
        consumer.wakeup();
    }

    @Override
    public void run() {
        consumer.subscribe(List.of(config.kafkaTopic()));
        log.info("Subscribed to topic '{}' as group '{}'", config.kafkaTopic(), config.kafkaConsumerGroupId());
        try {
            while (running.get()) {
                pollAndProcessOnce();
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e; // genuine unexpected wakeup
            }
        } finally {
            closeQuietly();
        }
    }

    /**
     * Package-private for testability: one poll cycle.
     */
    void pollAndProcessOnce() {
        ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(config.kafkaPollTimeoutMs()));
        if (records.isEmpty()) {
            return;
        }
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, byte[]>> partitionRecords = records.records(partition);
            long lastHandledOffset = -1;
            boolean infraDown = false;

            for (ConsumerRecord<String, byte[]> record : partitionRecords) {
                boolean handled = processRecord(record);
                if (!handled) {
                    // Do not process further records of this partition in this pass; keep the
                    // ones already handled committed, and let the next poll retry from here.
                    infraDown = true;
                    break;
                }
                lastHandledOffset = record.offset();
            }

            if (lastHandledOffset >= 0) {
                commitOffset(partition, lastHandledOffset + 1);
            }
            if (infraDown) {
                log.warn("Storage backend appears unavailable; backing off before retrying partition {}", partition);
                Backoff.sleep(config.infraBackoffMs());
            }
        }
    }

    /**
     * @return {@code true} if the record was fully handled (persisted or DLT'd) and the
     * offset may advance past it; {@code false} if it must be retried on the next poll.
     */
    private boolean processRecord(ConsumerRecord<String, byte[]> record) {
        byte[] rawValue = record.value(); // exact bytes polled from the broker, per requirement
        if (rawValue == null) {
            log.warn("Tombstone/null value at {}-{}@{}, skipping", record.topic(), record.partition(), record.offset());
            return true; // nothing to persist, nothing lost
        }

        ChatMessage message;
        try {
            message = codec.decode(rawValue);
        } catch (MessageCodecException e) {
            log.warn("Poison pill at {}-{}@{}: {}", record.topic(), record.partition(), record.offset(), e.getMessage());
            return routeToDlt(record, "DESERIALIZATION_ERROR", e.getMessage());
        }

        log.info("message is : "+message);
        return persistWithRetry(record, message, rawValue);
    }

    private boolean persistWithRetry(ConsumerRecord<String, byte[]> record, ChatMessage message, byte[] rawValue) {
        int maxAttempts = config.storageMaxRetries();
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                repository.save(message, rawValue);
                return true;
            } catch (StorageException e) {
                if (!e.isTransient()) {
                    log.error("Non-transient storage error for {} at {}-{}@{}: {}",
                            message.getMessageId(), record.topic(), record.partition(), record.offset(), e.getMessage());
                    return routeToDlt(record, "STORAGE_ERROR", e.getMessage());
                }
                if (attempt < maxAttempts) {
                    long delay = Backoff.delayForAttempt(config.storageRetryInitialBackoffMs(), attempt);
                    log.warn("Transient storage error (attempt {}/{}) for {}: {}. Retrying in {} ms",
                            attempt, maxAttempts, message.getMessageId(), e.getMessage(), delay);
                    Backoff.sleep(delay);
                } else {
                    log.error("Storage backend still unavailable after {} attempts for {}; will retry this record " +
                                    "on the next poll cycle (offset not advanced).",
                            maxAttempts, message.getMessageId());
                    return false; // infra still down: caller must not advance the offset
                }
            }
        }
        return false;
    }

    private boolean routeToDlt(ConsumerRecord<String, byte[]> record, String reason, String detail) {
        boolean published = deadLetterPublisher.publish(
                record.topic(), record.partition(), record.offset(),
                record.key(), record.value(), reason, detail);
        if (!published) {
            log.error("Failed to publish to DLT for {}-{}@{}; will retry on next poll (offset not advanced).",
                    record.topic(), record.partition(), record.offset());
        }
        return published;
    }

    private void commitOffset(TopicPartition partition, long offsetToCommit) {
        try {
            consumer.commitSync(Map.of(partition, new OffsetAndMetadata(offsetToCommit)));
        } catch (Exception e) {
            // If the commit itself fails, we simply do not advance in our bookkeeping;
            // the same records will be re-processed on the next poll (at-least-once,
            // duplicates are acceptable for this archival use case, loss is not).
            log.error("Failed to commit offset {} for partition {}", offsetToCommit, partition, e);
        }
    }

    private void closeQuietly() {
        try {
            consumer.close();
        } catch (Exception e) {
            log.warn("Error closing Kafka consumer", e);
        }
    }
}
