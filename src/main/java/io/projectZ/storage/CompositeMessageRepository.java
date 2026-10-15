
package io.projectZ.storage;

import io.projectZ.model.ChatMessage;

import java.util.Objects;

public final class CompositeMessageRepository implements MessageRepository {

    private final MessageRepository redisRepository;
    private final MessageRepository mongoRepository;

    public CompositeMessageRepository(
            MessageRepository redisRepository,
            MessageRepository mongoRepository) {

        this.redisRepository = Objects.requireNonNull(redisRepository);
        this.mongoRepository = Objects.requireNonNull(mongoRepository);
    }

    @Override
    public void save(ChatMessage message, byte[] rawPayload)
            throws StorageException {

        // First persist to Redis.
        redisRepository.save(message, rawPayload);

        // Then persist to MongoDB.
        // If this fails, the Kafka offset must not advance.
        // A retry will safely repeat the Redis write and retry MongoDB.
        mongoRepository.save(message, rawPayload);
    }

    @Override
    public boolean isHealthy() {
        return redisRepository.isHealthy()
                && mongoRepository.isHealthy();
    }

    @Override
    public void close() {
        closeQuietly(redisRepository);
        closeQuietly(mongoRepository);
    }

    private void closeQuietly(MessageRepository repository) {
        try {
            repository.close();
        } catch (Exception ignored) {
            // Best-effort cleanup.
        }
    }
}
