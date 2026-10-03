package io.projectZ.storage;

import io.projectZ.config.AppConfig;
import io.projectZ.consumer.MessagePersistenceConsumer;

/**
 * Chooses a {@link MessageRepository} implementation based on {@code storage.type}.
 * <p>
 * To add a new database, e.g. Postgres:
 * <ol>
 *   <li>Add a new class {@code PostgresMessageRepository implements MessageRepository}.</li>
 *   <li>Add one {@code case "postgres" -> new PostgresMessageRepository(config);} line below.</li>
 * </ol>
 * Nothing else in the codebase changes: {@link MessagePersistenceConsumer}
 * only ever sees the {@link MessageRepository} interface. This is the concrete mechanism behind
 * the "cheap to add a new database" requirement.
 */
public final class MessageRepositoryFactory {

    private MessageRepositoryFactory() {
    }

    public static MessageRepository create(AppConfig config) {
        String type = config.storageType().toLowerCase();
        return switch (type) {
            case "redis" -> new RedisMessageRepository(config);
            default -> throw new IllegalArgumentException("Unsupported storage.type: " + type);
        };
    }
}
