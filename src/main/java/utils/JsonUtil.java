package utils;

import Dto.Transaction;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility class for JSON serialization and deserialization operations.
 * <p>
 * This class provides a centralized mechanism for converting application DTOs (Data Transfer Objects)
 * into JSON strings, primarily used for serializing data before sending it to external systems
 * (e.g., Elasticsearch, Kafka).
 * <p>
 * <strong>Thread Safety Note:</strong>
 * The {@link ObjectMapper} instance is static and shared. According to Jackson documentation,
 * ObjectMapper is thread-safe as long as it is configured before use and not modified concurrently.
 */
public class JsonUtil {

    /**
     * Shared ObjectMapper instance.
     * <p>
     * Reusing a single instance of ObjectMapper is a best practice for performance,
     * as initialization is expensive.
     */
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Serializes a {@link Transaction} object into its JSON string representation.
     *
     * @param transaction The transaction DTO to be serialized.
     * @return The JSON string representation of the transaction, or {@code null} if serialization fails.
     */
    public static String convertTransactionToJson(Transaction transaction) {
        try {
            return objectMapper.writeValueAsString(transaction);
        } catch (JsonProcessingException e) {
            // Log the error deeply to aid debugging without crashing the stream processing.
            // In a production environment, consider using a structured logger (e.g., SLF4J)
            // instead of standard error stream printing.
            System.err.println("Failed to serialize transaction ID: " + transaction.getTransactionId());
            e.printStackTrace();
            
            // Return null to indicate failure. The caller must handle potential null values
            // to avoid NullPointerException downstream.
            return null;
        }
    }
}