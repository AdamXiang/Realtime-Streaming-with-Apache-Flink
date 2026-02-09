package Dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor; // Suggestion: Added for Flink POJO compliance

import java.sql.Date;

/**
 * Data Transfer Object (DTO) representing the aggregated sales metrics per product category.
 * <p>
 * <strong>Role in Topology:</strong>
 * This class serves as the state accumulator in the "Sales Per Category" Flink job.
 * It carries the rolling sum of sales for a specific date and category, effectively mapping
 * to the `sales_per_category` table in the target database.
 * <p>
 * <strong>Serialization Note:</strong>
 * Used by Flink's serialization stack. To ensure efficient serialization (avoiding Kryo fallback),
 * this class should adhere to Flink's POJO requirements (public class, public no-args constructor,
 * and public getters/setters).
 */
@Data
@AllArgsConstructor
// @NoArgsConstructor // TODO: [Optimization] Highly recommended adding this for Flink PojoSerializer support.
public class SalesPerCategory {

    /**
     * The business date of the transactions (Event Time).
     * <p>
     * Utilizes {@link java.sql.Date} to ensure direct compatibility with JDBC PreparedStatement
     * and the target PostgreSQL `DATE` column type.
     */
    private Date transactionDate;

    /**
     * The product category name (e.g., "Electronics", "Clothing").
     * Acts as part of the Composite Key (TransactionDate + Category) for aggregation.
     */
    private String category;

    /**
     * The aggregated total sales amount.
     * <p>
     * <strong>Engineering Note (Precision):</strong>
     * Currently using {@code Double} for performance and simplicity in this demo.
     * <p>
     * <strong>Warning:</strong> In production financial systems, binary floating-point types (Double/Float)
     * can lead to precision loss during accumulation. Consider using {@link java.math.BigDecimal}
     * for strict monetary calculations.
     */
    private Double totalSales;
}