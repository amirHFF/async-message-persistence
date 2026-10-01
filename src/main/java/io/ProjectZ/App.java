package io.ProjectZ;

import io.ProjectZ.codec.BinaryMessageCodec;
import io.ProjectZ.codec.MessageCodec;
import io.ProjectZ.config.AppConfig;
import io.ProjectZ.consumer.MessagePersistenceConsumer;
import io.ProjectZ.dlt.DeadLetterPublisher;
import io.ProjectZ.dlt.KafkaDeadLetterPublisher;
import io.ProjectZ.storage.MessageRepository;
import io.ProjectZ.storage.MessageRepositoryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public final class App {

    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        AppConfig config = new AppConfig();
        DeadLetterPublisher deadLetterPublisher = new KafkaDeadLetterPublisher(config);

        int threadCount = config.getInt("consumer.thread.count", 2);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount, r -> {
            Thread t = new Thread(r);
            t.setName("chat-message-persistence-consumer-" + t.getId());
            return t;
        });

        List<MessagePersistenceConsumer> consumers = new ArrayList<>();
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            MessageCodec codec = new BinaryMessageCodec();
            MessageRepository repo = MessageRepositoryFactory.create(config); // هر ترد، repository مستقل خودش
            MessagePersistenceConsumer consumer =
                    new MessagePersistenceConsumer(config, codec, repo, deadLetterPublisher);
            consumers.add(consumer);
            futures.add(executor.submit(consumer));
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received, stopping {} consumer(s)...", threadCount);
            consumers.forEach(MessagePersistenceConsumer::shutdown);
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            closeQuietly(deadLetterPublisher);
            log.info("Shutdown complete.");
        }, "shutdown-hook"));

        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (ExecutionException e) {
                log.error("A consumer thread terminated unexpectedly", e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            log.warn("Error while closing {}", closeable.getClass().getSimpleName(), e);
        }
    }
}
