package io.projectZ.codec;

import io.projectZ.avroSchema.ChatMessageAvro;
import io.projectZ.model.ChatMessage;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * {@link MessageCodec} implementation backed by Apache Avro binary encoding
 * (org.apache.avro.io.BinaryEncoder / BinaryDecoder - NOT Avro's JSON encoder),
 * using the generated {@link ChatMessageAvro} SpecificRecord class rather than a
 * hand-assembled {@code GenericRecord}.
 * <p>
 * {@link ChatMessageAvro} is generated at build time by the {@code avro-maven-plugin}
 * from {@code src/main/avro/ChatMessage.avsc} - it is not written or edited by hand.
 * The schema is the single source of truth for the wire format; this class only maps
 * between {@link ChatMessageAvro} (the wire representation) and {@link ChatMessage}
 * (the domain model the rest of the pipeline works with).
 * <p>
 * {@code rawPayload} is Avro {@code bytes} (opaque), not {@code string}: today it carries
 * text, but is expected to carry voice/image data later without needing a schema or
 * codec change - only whatever consumes {@code rawPayload} downstream needs to interpret
 * it. When that happens, prefer adding a {@code contentType} field with an Avro
 * {@code default} to {@code ChatMessage.avsc} over changing this class.
 * <p>
 * No schema registry is used: the schema compiled into {@link ChatMessageAvro} is used
 * as both the writer's and reader's schema. If the schema evolves later without a
 * registry, follow Avro's evolution rules (new fields need a {@code default}) and keep
 * old messages readable by the current generated class.
 */
public final class BinaryMessageCodec implements MessageCodec {

    private final SpecificDatumWriter<ChatMessageAvro> writer = new SpecificDatumWriter<>(ChatMessageAvro.class);
    private final SpecificDatumReader<ChatMessageAvro> reader = new SpecificDatumReader<>(ChatMessageAvro.class);

    @Override
    public ChatMessage decode(byte[] rawPayload) throws MessageCodecException {
        if (rawPayload == null || rawPayload.length == 0) {
            throw new MessageCodecException("Payload is null or empty");
        }
        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(rawPayload, null);
            ChatMessageAvro avroMessage = reader.read(null, decoder);

            String messageId = avroMessage.getMessageId();
            String conversationId = avroMessage.getConversationId();
            String senderId = avroMessage.getSenderId();
            String receiverId = avroMessage.getReceiverId();
            long timestamp = avroMessage.getTimestamp();

            ByteBuffer payloadBuffer = avroMessage.getRawPayload();
            byte[] payload = new byte[payloadBuffer.remaining()];
            payloadBuffer.get(payload);

            return new ChatMessage(messageId, conversationId, senderId, receiverId, timestamp, payload);
        } catch (IOException | AvroRuntimeException | IllegalArgumentException e) {
            // IllegalArgumentException: UUID.fromString on a malformed id.
            // AvroRuntimeException/IOException: truncated or otherwise malformed Avro bytes.
            throw new MessageCodecException("Failed to decode Avro payload", e);
        }
    }

    /** Convenience encoder, primarily useful for tests / producers exercising this codec. */
    public byte[] encode(ChatMessage message) {
        ChatMessageAvro avroMessage = ChatMessageAvro.newBuilder()
                .setMessageId(message.getMessageId())
                .setConversationId(message.getConversationId())
                .setSenderId(message.getSenderId())
                .setReceiverId(message.getReceiverId())
                .setTimestamp(message.getTimestampEpochMillis())
                .setRawPayload(ByteBuffer.wrap(message.getRawPayload()))
                .build();

        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(avroMessage, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            // Encoding to an in-memory ByteArrayOutputStream cannot realistically fail;
            // wrapped only to satisfy the checked-exception signature of DatumWriter#write.
            throw new IllegalStateException("Unexpected failure encoding ChatMessage to Avro", e);
        }
    }
}
