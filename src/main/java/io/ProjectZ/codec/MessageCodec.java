package io.ProjectZ.codec;

import io.ProjectZ.model.ChatMessage;

/**
 * Translates the raw {@code byte[]} payload polled from Kafka into a {@link ChatMessage}.
 * <p>
 * Kept as an interface so the wire format can change (or a second format can be supported)
 * without touching the consumer/retry/storage/DLT pipeline - only a new implementation is
 * required (Open/Closed).
 */
public interface MessageCodec {

    /**
     * @param rawPayload the exact bytes polled from the Kafka record value. Must not be mutated.
     * @return the decoded message
     * @throws MessageCodecException if the payload is malformed / cannot be decoded.
     *                                This is treated as a "poison pill" upstream and routed
     *                                straight to the DLT (retrying will not help).
     */
    ChatMessage decode(byte[] rawPayload) throws MessageCodecException;
}
