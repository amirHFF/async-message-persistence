package io.projectZ.codec;

import io.projectZ.model.ChatMessage;

/** Thrown when a raw payload cannot be decoded into a {@link ChatMessage}. */
public class MessageCodecException extends Exception {

    public MessageCodecException(String message) {
        super(message);
    }

    public MessageCodecException(String message, Throwable cause) {
        super(message, cause);
    }
}
