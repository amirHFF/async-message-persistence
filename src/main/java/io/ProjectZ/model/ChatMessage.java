package io.ProjectZ.model;

import io.ProjectZ.storage.MessageRepository;

import java.util.Arrays;
import java.util.Objects;

/**
 * In-memory representation of a chat message decoded from the Kafka payload.
 * <p>
 * {@code rawPayload} is kept as raw bytes rather than a {@code String} on purpose:
 * today it always carries UTF-8 text, but it is expected to eventually carry voice or
 * image data too. Keeping it as {@code byte[]} here means this class - and everything
 * upstream of it (codec, consumer, storage, DLT) - does not need to change when that
 * happens; only whatever downstream consumer interprets {@code rawPayload} needs to know
 * the content's actual type.
 * <p>
 * NOTE: this {@code rawPayload} (the message's own content bytes) is a different thing
 * from the {@code rawPayload} parameter on {@link MessageRepository#save},
 * which is the *entire* Avro-encoded Kafka record value (all fields, including this one,
 * still encoded). The naming overlap is coincidental; the storage layer's parameter is
 * named for what Kafka gave the consumer, this field is named for what the chat message
 * itself carries as content.
 */
public final class ChatMessage {

    private final String messageId;
    private final String conversationId;
    private final String senderId;
    private final String receiverId;
    private final long timestampEpochMillis;
    private final byte[] rawPayload;

    public ChatMessage(String messageId, String conversationId, String senderId, String receiverId,
                        long timestampEpochMillis, byte[] rawPayload) {
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        this.conversationId = Objects.requireNonNull(conversationId, "conversationId");
        this.senderId = Objects.requireNonNull(senderId, "senderId");
        this.receiverId = Objects.requireNonNull(receiverId, "receiverId");
        this.timestampEpochMillis = timestampEpochMillis;
        Objects.requireNonNull(rawPayload, "rawPayload");
        // Defensive copy: byte[] is mutable, unlike the old String content field, so we
        // must not hold onto the caller's array directly or ChatMessage's immutability
        // guarantee would be broken by a caller mutating the array after construction.
        this.rawPayload = Arrays.copyOf(rawPayload, rawPayload.length);
    }

    public String getMessageId() {
        return messageId;
    }

    public String getConversationId() {
        return conversationId;
    }

    public String getSenderId() {
        return senderId;
    }

    public String getReceiverId() {
        return receiverId;
    }

    public long getTimestampEpochMillis() {
        return timestampEpochMillis;
    }

    /** Defensive copy - callers must not be able to mutate the message's internal state. */
    public byte[] getRawPayload() {
        return Arrays.copyOf(rawPayload, rawPayload.length);
    }

    @Override
    public String toString() {
        return "ChatMessage{" +
                "messageId=" + messageId +
                ", conversationId=" + conversationId +
                ", senderId='" + senderId + '\'' +
                ", receiverId='" + receiverId + '\'' +
                ", timestampEpochMillis=" + timestampEpochMillis +
                ", rawPayloadLength=" + rawPayload.length +
                '}';
    }
}
