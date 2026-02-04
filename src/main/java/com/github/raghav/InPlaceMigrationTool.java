/*
 * Copyright 2025 Raghav Aggarwal
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.raghav;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool for performing in-place migration of Hive tables to Iceberg format.
 * This tool supports batch processing, dry runs, and automatic rollback on failure.
 */
public class InPlaceMigrationTool {

    private static final Logger logger = LoggerFactory.getLogger(InPlaceMigrationTool.class);

    /**
     * Represents the status of a migration operation.
     */
    public enum MigrationStatus {
        SUCCESS,
        FAILURE,
        SKIPPED
    }

    /**
     * Holds metadata information about a table.
     *
     * @param exists    Whether the table exists.
     * @param isManaged Whether the table is a managed Hive table.
     * @param isIceberg Whether the table is already in Iceberg format.
     * @param format    The file format of the table (e.g., parquet, orc).
     * @param location  The physical location of the table.
     */
    private record TableMetadata(
            boolean exists, boolean isManaged, boolean isIceberg, String format, String location) {}

    /**
     * Entry point for the migration tool.
     * Parses command line arguments and initiates the migration process.
     *
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        String inputTarget = "default";
        boolean dropBackup = false;
        boolean dryRun = false;
        boolean convertManaged = true;
        boolean skipValidation = false;
        int threads = 4;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--dry-run" -> dryRun = true;
                case "--drop-backup" -> dropBackup = true;
                case "--skip-managed" -> convertManaged = false;
                case "--skip-validation" -> skipValidation = true;
                case "--threads" -> {
                    if (i + 1 < args.length) threads = Integer.parseInt(args[++i]);
                }
                default -> {
                    if (!args[i].startsWith("--")) inputTarget = args[i];
                }
            }
        }

        try (var spark = SparkSession.builder()
                .appName("Iceberg-InPlaceMigration")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hive")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .enableHiveSupport()
                .getOrCreate()) {

            logger.info("Starting In-Place Migration for: {}", inputTarget);

            var tool = new InPlaceMigrationTool();
            List<String> tables = tool.discoverTables(spark, inputTarget);

            if (tables.isEmpty()) {
                logger.warn("No eligible tables found.");
                return;
            }

            var config = new MigrationConfig(tables, dropBackup, dryRun, convertManaged, skipValidation, threads);
            tool.runBatch(spark, config);
        }
    }

    /**
     * Executes the migration for a batch of tables using a thread pool.
     *
     * @param spark  The SparkSession.
     * @param config The migration configuration.
     */
    public void runBatch(SparkSession spark, MigrationConfig config) {
        logger.info(
                "Batch: {}. Threads: {}. DryRun: {}. SkipValidation: {}",
                config.tableList().size(),
                config.threadPoolSize(),
                config.dryRun(),
                config.skipValidation());

        AtomicInteger progress = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(config.threadPoolSize());

        try {
            List<CompletableFuture<MigrationResult>> futures = config.tableList().stream()
                    .map(table -> CompletableFuture.supplyAsync(
                            () -> {
                                MigrationResult res = migrateTable(spark, table, config);
                                System.out.printf(
                                        "[%d/%d] Processed %s: %s%n",
                                        progress.incrementAndGet(),
                                        config.tableList().size(),
                                        table,
                                        res.status());
                                return res;
                            },
                            executor))
                    .toList();

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            List<MigrationResult> results =
                    futures.stream().map(CompletableFuture::join).toList();

            printReport(results);

        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(1, TimeUnit.HOURS)) executor.shutdownNow();
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Migrates a single table to Iceberg format.
     * Performs analysis, validation, and handles rollback if necessary.
     *
     * @param spark     The SparkSession.
     * @param tableName The name of the table to migrate.
     * @param config    The migration configuration.
     * @return The result of the migration.
     */
    private MigrationResult migrateTable(SparkSession spark, String tableName, MigrationConfig config) {
        long start = System.currentTimeMillis();

        String[] parts = tableName.split("\\.");
        String db = parts.length > 1 ? parts[0] : "default";
        String tbl = parts.length > 1 ? parts[1] : tableName;

        String backupName = String.format("`%s`.`%s_hive_backup_%d`", db, tbl, System.currentTimeMillis());

        try {
            TableMetadata meta = analyzeTable(spark, tableName);

            if (!meta.exists()) {
                return new MigrationResult(tableName, MigrationStatus.FAILURE, 0, "Table not found");
            }

            if (meta.isIceberg()) {
                return new MigrationResult(tableName, MigrationStatus.SKIPPED, 0, "Already Iceberg");
            }

            if (!isMigratable(meta.format())) {
                return new MigrationResult(
                        tableName, MigrationStatus.SKIPPED, 0, "Unsupported format (" + meta.format() + ")");
            }

            if (meta.isManaged()) {
                if (!config.convertManagedToExternal()) {
                    return new MigrationResult(
                            tableName, MigrationStatus.SKIPPED, 0, "Table is MANAGED (Conversion disabled)");
                }
            }

            if (config.dryRun()) {
                return new MigrationResult(
                        tableName, MigrationStatus.SUCCESS, 0, "Dry Run: Ready (" + meta.format() + ")");
            }

            if (meta.isManaged()) {
                convertManagedToExternal(spark, tableName);
            }

            long preCount = -1;
            if (!config.skipValidation()) {
                preCount = spark.table(tableName).count();
            }

            // Execute Migrate
            logger.info("[{}] Executing Migrate Action...", tableName);
            SparkActions.get(spark)
                    .migrateTable(tableName)
                    .backupTableName(backupName) // Passing the Quoted Identifier
                    .tableProperties(Map.of(
                            "format-version", "2",
                            "migrated-at", Instant.now().toString(),
                            "migration-method", "in-place-migrate"))
                    .execute();

            // Validation
            if (!config.skipValidation()) {
                long postCount = spark.table(tableName).count();
                if (preCount != postCount) {
                    logger.error(
                            "[{}] Count Mismatch (Pre: {}, Post: {}). Rolling back...", tableName, preCount, postCount);
                    performRollback(spark, tableName, backupName);
                    return new MigrationResult(
                            tableName,
                            MigrationStatus.FAILURE,
                            System.currentTimeMillis() - start,
                            "Validation Failed: Data mismatch. Rolled back.");
                }
            }

            String cleanupMsg = "";
            if (config.dropBackup()) {
                boolean dropped = safeDrop(spark, backupName);
                cleanupMsg = dropped ? " (Backup Dropped)" : " (Backup Drop Failed)";
            } else {
                cleanupMsg = " (Backup: " + backupName + ")";
            }

            return new MigrationResult(
                    tableName,
                    MigrationStatus.SUCCESS,
                    System.currentTimeMillis() - start,
                    "Success. Format: " + meta.format() + cleanupMsg);

        } catch (Exception e) {
            logger.error("[{}] Migration Failed", tableName, e);
            try {
                if (!spark.catalog().tableExists(tableName)
                        && spark.catalog().tableExists(backupName.replace("`", ""))) {
                    performRollback(spark, tableName, backupName);
                    return new MigrationResult(
                            tableName,
                            MigrationStatus.FAILURE,
                            System.currentTimeMillis() - start,
                            "Error: " + e.getMessage() + " (Rolled Back)");
                }
            } catch (Exception rollbackEx) {
                logger.error("Failed during rollback check", rollbackEx);
            }
            return new MigrationResult(
                    tableName, MigrationStatus.FAILURE, System.currentTimeMillis() - start, "Error: " + e.getMessage());
        }
    }

    /**
     * Analyzes a table to gather metadata such as format, type, and location.
     *
     * @param spark     The SparkSession.
     * @param tableName The name of the table to analyze.
     * @return The metadata of the table.
     */
    private TableMetadata analyzeTable(SparkSession spark, String tableName) {
        try {
            if (!spark.catalog().tableExists(tableName)) {
                return new TableMetadata(false, false, false, "unknown", "");
            }

            List<Row> rows = spark.sql("DESCRIBE EXTENDED " + tableName).collectAsList();

            String format = "unknown";
            boolean isManaged = false;
            boolean isIceberg = false;
            String location = "";

            for (Row r : rows) {
                if (r.size() < 2) continue;
                String key = r.getString(0) != null ? r.getString(0).trim() : "";
                String val = r.getString(1);

                if (key.equals("InputFormat")) {
                    String lower = val.toLowerCase();
                    if (lower.contains("iceberg")) isIceberg = true;
                    else if (lower.contains("parquet")) format = "parquet";
                    else if (lower.contains("orc")) format = "orc";
                    else if (lower.contains("avro")) format = "avro";
                    else format = val;
                } else if (key.equals("Provider")) {
                    if (val.equalsIgnoreCase("iceberg")) isIceberg = true;
                } else if (key.equals("Type")) {
                    if (val.toUpperCase().contains("MANAGED")) isManaged = true;
                } else if (key.equals("Location")) {
                    location = val;
                } else if (key.equals("Table Properties")) {
                    if (val != null && val.toUpperCase().contains("TABLE_TYPE=ICEBERG")) {
                        isIceberg = true;
                    }
                }
            }

            return new TableMetadata(true, isManaged, isIceberg, format, location);

        } catch (Exception e) {
            logger.warn("Failed to analyze table {}", tableName, e);
            return new TableMetadata(false, false, false, "error", "");
        }
    }

    /**
     * Rolls back a migration by restoring the original table from the backup.
     *
     * @param spark        The SparkSession.
     * @param originalName The original name of the table.
     * @param backupName   The name of the backup table.
     */
    private void performRollback(SparkSession spark, String originalName, String backupName) {
        try {
            logger.warn("[{}] Attempting rollback...", originalName);
            spark.sql("DROP TABLE IF EXISTS " + originalName);
            // Check if backup exists (handling quotes potentially)
            if (spark.catalog().tableExists(backupName.replace("`", ""))) {
                spark.sql(String.format("ALTER TABLE %s RENAME TO %s", backupName, originalName));
                logger.info("[{}] Rollback successful.", originalName);
            }
        } catch (Exception e) {
            logger.error("[{}] Rollback failed.", originalName, e);
        }
    }

    /**
     * Safely drops a table, ensuring it is set to external before dropping.
     *
     * @param spark     The SparkSession.
     * @param tableName The name of the table to drop.
     * @return True if dropped successfully, false otherwise.
     */
    private boolean safeDrop(SparkSession spark, String tableName) {
        try {
            // Remove quotes for catalog check if necessary, but DROP usually handles them
            String cleanName = tableName.replace("`", "");
            if (!spark.catalog().tableExists(cleanName)) return true;

            spark.sql(String.format("ALTER TABLE %s SET TBLPROPERTIES('EXTERNAL'='TRUE')", tableName));
            spark.sql("DROP TABLE " + tableName);
            return true;
        } catch (Exception e) {
            logger.warn("Failed to drop backup " + tableName, e);
            return false;
        }
    }

    /**
     * Converts a managed Hive table to an external table.
     *
     * @param spark     The SparkSession.
     * @param tableName The name of the table to convert.
     */
    private void convertManagedToExternal(SparkSession spark, String tableName) {
        try {
            spark.sql(String.format("ALTER TABLE %s SET TBLPROPERTIES('EXTERNAL'='TRUE')", tableName));
            logger.info("[{}] Converted to EXTERNAL.", tableName);
        } catch (Exception e) {
            logger.warn("[{}] Failed to set External status.", tableName, e);
        }
    }

    /**
     * Checks if the table format is supported for migration.
     *
     * @param format The format of the table (e.g., parquet, orc, avro).
     * @return True if migratable, false otherwise.
     */
    private boolean isMigratable(String format) {
        return "parquet".equals(format) || "orc".equals(format) || "avro".equals(format);
    }

    /**
     * Discovers tables based on the input target (database or comma-separated list).
     *
     * @param spark The SparkSession.
     * @param input The input target string.
     * @return A list of table names.
     */
    public List<String> discoverTables(SparkSession spark, String input) {
        if (input.contains(",")) {
            return Arrays.stream(input.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();
        }
        if (input.contains(".")) {
            return List.of(input);
        }
        try {
            return spark.catalog().listTables(input).collectAsList().stream()
                    .filter(t -> !t.tableType().equalsIgnoreCase("VIEW"))
                    .map(t -> input + "." + t.name())
                    .toList();
        } catch (Exception e) {
            logger.error("Error scanning DB", e);
            return List.of();
        }
    }

    /**
     * Prints a summary report of the migration results.
     *
     * @param results The list of migration results.
     */
    private void printReport(List<MigrationResult> results) {
        System.out.println("\n" + "=".repeat(95));
        System.out.println(" MIGRATION REPORT");
        System.out.println("=".repeat(95));
        System.out.printf("%-30s | %-10s | %s%n", "Table", "Status", "Message");
        System.out.println("-".repeat(95));

        results.forEach(res -> System.out.printf("%-30s | %-10s | %s%n", res.tableName(), res.status(), res.message()));

        System.out.println("=".repeat(95));
        long success = results.stream()
                .filter(r -> r.status() == MigrationStatus.SUCCESS)
                .count();
        long failed = results.stream()
                .filter(r -> r.status() == MigrationStatus.FAILURE)
                .count();
        long skipped = results.stream()
                .filter(r -> r.status() == MigrationStatus.SKIPPED)
                .count();
        System.out.printf(
                "Total: %d | Success: %d | Failed: %d | Skipped: %d%n", results.size(), success, failed, skipped);
    }
}
