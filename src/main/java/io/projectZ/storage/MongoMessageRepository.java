
package io.projectZ.storage;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.Binary;

import io.projectZ.model.ChatMessage;

import java.util.Arrays;
import java.util.Date;
import java.util.Objects;

public final class MongoMessageRepository implements MessageRepository {

    private final MongoClient mongoClient;
    private final MongoDatabase database;
    private final MongoCollection<Document> collection;

    public MongoMessageRepository(
            String connectionString,
            String databaseName,
            String collectionName) {

        Objects.requireNonNull(connectionString, "connectionString");
        Objects.requireNonNull(databaseName, "databaseName");
        Objects.requireNonNull(collectionName, "collectionName");

        this.mongoClient = MongoClients.create(connectionString);
        this.database = mongoClient.getDatabase(databaseName);

        this.collection = database
                .getCollection(collectionName)
                .withWriteConcern(WriteConcern.ACKNOWLEDGED.withJournal(true));
    }

    @Override
    public void save(ChatMessage message, byte[] rawPayload)
            throws StorageException {

        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(rawPayload, "rawPayload");

        Document document = new Document("_id", message.getMessageId())
                .append("messageId", message.getMessageId())
                .append("conversationId", message.getConversationId())
                .append("senderId", message.getSenderId())
                .append("receiverId", message.getReceiverId())
                .append("timestampEpochMillis", message.getTimestampEpochMillis())
                .append("timestamp", new Date(message.getTimestampEpochMillis()))
                .append("messagePayload", new Binary(message.getRawPayload()))
                .append("rawKafkaPayload", new Binary(rawPayload))
                .append("storageFormat", "AVRO");

        try {
            collection.insertOne(document);

        } catch (MongoWriteException e) {

            // Kafka may redeliver a message after a successful write
            // if committing the offset failed.
            if (e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
                verifyDuplicate(message.getMessageId(), rawPayload);
                return;
            }

            throw new StorageException(
                    "MongoDB write failed for message " + message.getMessageId(),
                    e,
                    false);

        } catch (MongoException e) {
            throw new StorageException(
                    "MongoDB operation failed for message " + message.getMessageId(),
                    e,
                    isTransientFailure(e));
        }
    }

    private void verifyDuplicate(String messageId, byte[] rawPayload)
            throws StorageException {

        try {
            Document existing = collection
                    .find(Filters.eq("_id", messageId))
                    .first();

            if (existing == null) {
                throw new StorageException(
                        "Duplicate message ID reported but document was not found: "
                                + messageId,
                        null,
                        true);
            }

            Binary storedPayload = existing.get("rawKafkaPayload", Binary.class);

            if (storedPayload != null
                    && Arrays.equals(storedPayload.getData(), rawPayload)) {
                // Identical record already exists: safe Kafka redelivery.
                return;
            }

            throw new StorageException(
                    "Message ID already exists with different Kafka payload: "
                            + messageId,
                    null,
                    false);

        } catch (StorageException e) {
            throw e;
        } catch (MongoException e) {
            throw new StorageException(
                    "Could not verify duplicate message " + messageId,
                    e,
                    isTransientFailure(e));
        }
    }

    private boolean isTransientFailure(MongoException e) {
        return e instanceof MongoSocketException
                || e instanceof MongoTimeoutException;
    }

    @Override
    public boolean isHealthy() {
        try {
            database.runCommand(new Document("ping", 1));
            return true;
        } catch (MongoException e) {
            return false;
        }
    }

    @Override
    public void close() {
        mongoClient.close();
    }
}

