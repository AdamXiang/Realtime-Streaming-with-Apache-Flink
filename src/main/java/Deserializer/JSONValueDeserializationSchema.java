package Deserializer;

import Dto.Transaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Custom DeserializationSchema implementation for the Flink DataStream API.
 * <p>
 * This schema is responsible for parsing raw byte arrays (e.g., from Kafka) into
 * {@link Transaction} Data Transfer Objects (DTOs) using the Jackson library.
 * It serves as the bridge between the external messaging system and the Flink topology.
 */
public class JSONValueDeserializationSchema implements DeserializationSchema<Transaction> {

    /**
     * Jackson ObjectMapper instance used for JSON parsing.
     * Note: This instance is initialized inline. In complex scenarios, consider initializing
     * non-serializable fields inside the {@link #open(InitializationContext)} method
     * to ensure proper serialization across the Flink cluster.
     */
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Initialization hook for the deserialization schema.
     * This method is called during the setup of the parallel task.
     *
     * @param context Contextual information that can be used during initialization.
     * @throws Exception if initialization fails.
     */
    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
        // Initialization logic for non-serializable resources can be placed here if needed.
    }

    /**
     * Deserializes the incoming byte message into a {@link Transaction} object.
     *
     * @param bytes The raw byte array message from the source (e.g., Kafka record value).
     * @return The deserialized Transaction POJO.
     * @throws IOException If the byte array is not a valid JSON or cannot be mapped to the Transaction class.
     */
    @Override
    public Transaction deserialize(byte[] bytes) throws IOException {
        // Utilizing Jackson to map bytes directly to the POJO
        return objectMapper.readValue(bytes, Transaction.class);
    }

    /**
     * Determines whether the stream has reached its end.
     * <p>
     * For real-time streaming applications (e.g., Kafka consumers), this typically returns false
     * as the stream is unbounded and continuous.
     *
     * @param transaction The last deserialized element.
     * @return false, indicating the stream is unbounded.
     */
    @Override
    public boolean isEndOfStream(Transaction transaction) {
        return false;
    }

    /**
     * Specifies the TypeInformation for the produced data.
     * <p>
     * Flink needs this metadata to efficiently serialize/deserialize objects between operators
     * and to handle Java type erasure.
     *
     * @return TypeInformation ensuring Flink treats the data as a {@link Transaction} type.
     */
    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeInformation.of(Transaction.class);
    }
}