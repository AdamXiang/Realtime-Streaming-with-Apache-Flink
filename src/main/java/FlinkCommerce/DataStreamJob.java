/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package FlinkCommerce;

import Deserializer.JSONValueDeserializationSchema;
import Dto.SalesPerCategory;
import Dto.SalesPerDay;
import Dto.SalesPerMonth;
import Dto.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Date;

import static utils.JsonUtil.convertTransactionToJson;

/**
 * Main entry point for the Flink E-commerce Real-time Streaming Job.
 * <p>
 * This job constructs a topology that:
 * 1. Consumes financial transactions from a Kafka topic.
 * 2. Deserializes the JSON messages into Transaction objects.
 * 3. Sinks raw data into PostgreSQL and Elasticsearch for indexing.
 * 4. Performs real-time aggregations (Sales per Category, Day, Month) and upserts results to PostgreSQL.
 * <p>
 * Key Components:
 * - Source: Kafka (Topic: financial_transactions)
 * - Sink 1: PostgreSQL (Analytical / Relational Data)
 * - Sink 2: Elasticsearch (Search / Raw Data Inspector)
 */
public class DataStreamJob {

    // TODO: [Refactor] Externalize configuration to Environment Variables or Config Maps (Kubernetes).
    // Hardcoding credentials violates 12-Factor App principles and poses a security risk.
    private static final String JDBC_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "postgres";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String ELASTICSEARCH_HOST = "localhost";

    public static void main(String[] args) throws Exception {
        // Initialize the streaming execution context
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // =================================================================================
        // 1. Fault Tolerance & Resilience Configuration
        // =================================================================================
        /*
         * Enable Checkpointing every 5000ms.
         * Rationale: In a production environment, this allows the job to recover from failures
         * (e.g., TaskManager crash) by restoring the state (aggregations) from the last successful checkpoint.
         * Without this, all in-memory aggregation data would be lost upon restart.
         */
        env.enableCheckpointing(5000);
        
        // Ensure data consistency. This guarantees that each record is processed effectively once
        // regarding the state backend, preventing double-counting in aggregations.
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        String topic = "financial_transactions";

        // =================================================================================
        // 2. Source Configuration (Kafka)
        // =================================================================================
        KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(topic)
                .setGroupId("flink-group")
                // Start from the earliest offset to replay historical data if the group is new.
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JSONValueDeserializationSchema())
                .build();

        // Note: WatermarkStrategy.noWatermarks() is currently used. 
        // If windowing (Tumbling/Sliding windows) is introduced later, a proper WatermarkStrategy
        // based on transaction.transactionDate must be implemented to handle out-of-order events.
        DataStream<Transaction> transactionStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // =================================================================================
        // 3. Database Sink Configuration
        // =================================================================================
        // Configure JDBC batch execution to optimize throughput and reduce database load.
        JdbcExecutionOptions execOptions = new JdbcExecutionOptions.Builder()
                .withBatchSize(1000)      // Flush every 1000 records
                .withBatchIntervalMs(200) // Or flush every 200ms
                .withMaxRetries(5)        // Retry connection failures
                .build();

        JdbcConnectionOptions connOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(JDBC_URL)
                .withDriverName("org.postgresql.Driver")
                .withUsername(USERNAME)
                .withPassword(PASSWORD)
                .build();

        // ---------------------------------------------------------------------------------
        // DDL Initialization (Dev/Test Helper)
        // NOTE: In Production, schema management should be handled by tools like Flyway or Liquibase.
        // ---------------------------------------------------------------------------------
        // ... (DDL Sinks code hidden for brevity, assuming existing logic) ...


        // =================================================================================
        // 4. Raw Data Sink (Idempotent Upsert)
        // =================================================================================
        /*
         * Strategy: UPSERT
         * We use "ON CONFLICT (transaction_id) DO UPDATE" to handle duplicate message delivery.
         * Even with Exactly-Once checkpointing, sinks might receive duplicates during recovery.
         * Idempotent sinks are the last line of defense for data consistency.
         */
        transactionStream.addSink(JdbcSink.sink(
                "INSERT INTO transactions(transaction_id, product_id, product_name, product_category, product_price, " +
                        "product_quantity, product_brand, total_amount, currency, customer_id, transaction_date, payment_method) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (transaction_id) DO UPDATE SET " +
                        "product_id = EXCLUDED.product_id, " + // ... updates ...
                        "total_amount  = EXCLUDED.total_amount " +
                        "WHERE transactions.transaction_id = EXCLUDED.transaction_id",
                (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {
                    preparedStatement.setString(1, transaction.getTransactionId());
                    preparedStatement.setString(2, transaction.getProductId());
                    preparedStatement.setString(3, transaction.getProductName());
                    preparedStatement.setString(4, transaction.getProductCategory());
                    preparedStatement.setDouble(5, transaction.getProductPrice());
                    preparedStatement.setInt(6, transaction.getProductQuantity());
                    preparedStatement.setString(7, transaction.getProductBrand());
                    preparedStatement.setDouble(8, transaction.getTotalAmount());
                    preparedStatement.setString(9, transaction.getCurrency());
                    preparedStatement.setString(10, transaction.getCustomerId());
                    preparedStatement.setTimestamp(11, transaction.getTransactionDate());
                    preparedStatement.setString(12, transaction.getPaymentMethod());
                },
                execOptions,
                connOptions
        )).name("Sink: Raw Transactions (Postgres)");

        // =================================================================================
        // 5. Analytics Aggregations
        // =================================================================================

        // --- Aggregation: Sales Per Category ---
        transactionStream.map(transaction -> {
                    // ENGINEERING DECISION: EVENT TIME vs PROCESSING TIME
                    // We utilize transaction.getTransactionDate() instead of System.currentTimeMillis().
                    // This ensures that replaying historical Kafka logs results in aggregations
                    // assigned to the correct historical dates, not the current server time.
                    Date eventDate = new Date(transaction.getTransactionDate().getTime());
                    
                    return new SalesPerCategory(eventDate, transaction.getProductCategory(), transaction.getTotalAmount());
                })
                .keyBy(SalesPerCategory::getCategory)
                .reduce((current, newTrans) -> {
                    // Stateful Operation: This state is backed up by Flink Checkpoints.
                    current.setTotalSales(current.getTotalSales() + newTrans.getTotalSales());
                    return current;
                })
                .addSink(JdbcSink.sink(
                        "INSERT INTO sales_per_category(transaction_date, category, total_sales) " +
                                "VALUES (?, ?, ?) " +
                                "ON CONFLICT (transaction_date, category) DO UPDATE SET " +
                                "total_sales = EXCLUDED.total_sales",
                        (JdbcStatementBuilder<SalesPerCategory>) (ps, dto) -> {
                            ps.setDate(1, dto.getTransactionDate());
                            ps.setString(2, dto.getCategory());
                            ps.setDouble(3, dto.getTotalSales());
                        },
                        execOptions,
                        connOptions
                )).name("Sink: Sales Per Category");

        // --- Aggregation: Sales Per Day ---
        transactionStream.map(transaction -> {
                    Date eventDate = new Date(transaction.getTransactionDate().getTime());
                    return new SalesPerDay(eventDate, transaction.getTotalAmount());
                })
                .keyBy(SalesPerDay::getTransactionDate)
                .reduce((current, newTrans) -> {
                    current.setTotalSales(current.getTotalSales() + newTrans.getTotalSales());
                    return current;
                })
                .addSink(JdbcSink.sink(
                        "INSERT INTO sales_per_day(transaction_date, total_sales) " +
                                "VALUES (?,?) " +
                                "ON CONFLICT (transaction_date) DO UPDATE SET " +
                                "total_sales = EXCLUDED.total_sales",
                        (JdbcStatementBuilder<SalesPerDay>) (ps, dto) -> {
                            ps.setDate(1, dto.getTransactionDate());
                            ps.setDouble(2, dto.getTotalSales());
                        },
                        execOptions,
                        connOptions
                )).name("Sink: Sales Per Day");

        // --- Aggregation: Sales Per Month ---
        transactionStream.map(transaction -> {
                    // Extract Year and Month from the Event Time
                    LocalDate localDate = transaction.getTransactionDate().toLocalDateTime().toLocalDate();
                    int year = localDate.getYear();
                    int month = localDate.getMonthValue();
                    
                    return new SalesPerMonth(year, month, transaction.getTotalAmount());
                })
                // ENGINEERING DECISION: COMPOSITE KEY
                // Originally keyed by just 'Month', which causes collisions between years (e.g., Jan 2023 vs Jan 2024).
                // We construct a synthetic key "Year-Month" to ensure proper isolation of monthly data.
                .keyBy(dto -> dto.getYear() + "-" + dto.getMonth())
                .reduce((current, newTrans) -> {
                    current.setTotalSales(current.getTotalSales() + newTrans.getTotalSales());
                    return current;
                })
                .addSink(JdbcSink.sink(
                        "INSERT INTO sales_per_month(year, month, total_sales) " +
                                "VALUES (?,?,?) " +
                                "ON CONFLICT (year, month) DO UPDATE SET " +
                                "total_sales = EXCLUDED.total_sales",
                        (JdbcStatementBuilder<SalesPerMonth>) (ps, dto) -> {
                            ps.setInt(1, dto.getYear());
                            ps.setInt(2, dto.getMonth());
                            ps.setDouble(3, dto.getTotalSales());
                        },
                        execOptions,
                        connOptions
                )).name("Sink: Sales Per Month");

        // =================================================================================
        // 6. Search Engine Sink (Elasticsearch)
        // =================================================================================
        transactionStream.sinkTo(
                new Elasticsearch7SinkBuilder<Transaction>()
                        .setHosts(new HttpHost(ELASTICSEARCH_HOST, 9200, "http"))
                        .setEmitter((transaction, runtimeContext, requestIndexer) -> {
                            String json = convertTransactionToJson(transaction);
                            IndexRequest indexRequest = Requests.indexRequest()
                                    .index("transactions")
                                    // Use Transaction ID as Document ID for deduplication in ES
                                    .id(transaction.getTransactionId())
                                    .source(json, XContentType.JSON);
                            requestIndexer.add(indexRequest);
                        })
                        .build()
        ).name("Sink: Elasticsearch");

        // Execute the constructed DataStream topology
        env.execute("Flink Ecommerce Realtime Streaming");
    }
}