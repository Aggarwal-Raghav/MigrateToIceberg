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
 * InPlaceMigrationTool
 * <p>
 * A robust utility for migrating Apache Hive tables to Apache Iceberg using the "In-Place" strategy
 * via the official {@code migrate} procedure.
 * </p>
 *
 * <h3>Key Features:</h3>
 * <ul>
 * <li><b>Concurrency:</b> Processing is parallelized using a Fixed Thread Pool (JDK 17 compatible).</li>
 * <li><b>Safety First:</b> Automatically converts MANAGED tables to EXTERNAL to prevent accidental data loss.</li>
 * <li><b>Atomic Rollbacks:</b> If validation fails (row count mismatch), the tool automatically reverts changes.</li>
 * <li><b>Optimized Discovery:</b> Uses single-pass metadata analysis to reduce Hive Metastore load.</li>
 * <li><b>Smart Detection:</b> Skips tables that are already Iceberg, have unsupported formats, or are backup tables.</li>
 * </ul>
 *
 * <p><b>Compatibility:</b> JDK 17+, Spark 3.3+, Iceberg 1.4.3+</p>
 */
public class InPlaceMigrationTool {

    private static final Logger LOG = LoggerFactory.getLogger(InPlaceMigrationTool.class);

    /**
     * Represents the final outcome of a migration attempt.
     */
    public enum MigrationStatus {
        SUCCESS,
        FAILURE,
        SKIPPED
    }

    /**
     * Internal cache for table metadata to avoid multiple calls to the Metastore.
     */
    private record TableMetadata(
            boolean exists, boolean isManaged, boolean isIceberg, String format, String location) {}

    /**
     * Main Entry Point.
     * Parses command line arguments and initializes the Spark Session.
     *
     * @param args Command line arguments (e.g., target DB, flags like --dry-run).
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

            LOG.info("Starting In-Place Migration for: {}", inputTarget);

            var tool = new InPlaceMigrationTool();
            List<String> tables = tool.discoverTables(spark, inputTarget);

            if (tables.isEmpty()) {
                LOG.warn("No eligible tables found.");
                return;
            }

            var config = new MigrationConfig(tables, dropBackup, dryRun, convertManaged, skipValidation, threads);
            tool.runBatch(spark, config);
        }
    }

    /**
     * Orchestrates the batch migration process using a thread pool.
     * Uses CompletableFuture to handle async execution and result collection.
     *
     * @param spark  The active SparkSession.
     * @param config The migration configuration.
     */
    public void runBatch(SparkSession spark, MigrationConfig config) {
        LOG.info(
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

            // Block until all tasks complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            // Collect results
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
     * Core Logic: Migrates a single table.
     * <p>
     * Steps:
     * 1. Check existence and format compatibility.
     * 2. Check/Enforce EXTERNAL table status.
     * 3. Perform Pre-Migration Count (Validation).
     * 4. Execute `migrateTable` action.
     * 5. Perform Post-Migration Count (Validation).
     * 6. Cleanup backup table if configured.
     * </p>
     *
     * @param spark     The SparkSession.
     * @param tableName The fully qualified table name.
     * @param config    Runtime configuration.
     * @return MigrationResult containing status and messages.
     */
    private MigrationResult migrateTable(SparkSession spark, String tableName, MigrationConfig config) {
        long start = System.currentTimeMillis();

        // Fix: Extract simple table name to avoid naming issues and construct deterministic backup name
        String simpleTableName = tableName;
        String dbPrefix = "";
        if (tableName.contains(".")) {
            int lastDotIndex = tableName.lastIndexOf(".");
            simpleTableName = tableName.substring(lastDotIndex + 1);
            dbPrefix = tableName.substring(0, lastDotIndex + 1); // includes dot
        }

        // Deterministic backup name (no timestamp) to prevent accumulation of backups on re-runs
        String backupName = simpleTableName + "_hive_backup_";
        // Fully qualified backup name for catalog checks
        String fullBackupName = dbPrefix + backupName;

        try {
            LOG.info("[{}] Starting migration analysis...", tableName);

            // 1. Analyze Metadata (One-pass optimization)
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

            // Check if a backup table already exists from a previous run
            if (spark.catalog().tableExists(fullBackupName)) {
                LOG.warn("[{}] Existing backup table found: {}. Attempting to clear it.", tableName, fullBackupName);
                if (!safeDrop(spark, fullBackupName)) {
                    return new MigrationResult(
                            tableName, MigrationStatus.FAILURE, 0, "Failed to drop existing backup: " + fullBackupName);
                }
                LOG.info("[{}] Cleared existing backup table.", tableName);
            }

            // Check Managed Status
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

            // 2. Enforce External (Critical Safety Step)
            if (meta.isManaged()) {
                convertManagedToExternal(spark, tableName);
            }

            // 3. Pre-Migration Validation
            long preCount = -1;
            if (!config.skipValidation()) {
                LOG.info("[{}] Performing pre-migration count check...", tableName);
                preCount = spark.table(tableName).count();
            }

            // 4. Execute Migrate Action
            LOG.info("[{}] Executing Migrate Action. Backup: {}", tableName, backupName);
            SparkActions.get(spark)
                    .migrateTable(tableName)
                    .backupTableName(backupName)
                    .tableProperties(Map.of(
                            "format-version", "2",
                            "migrated-at", Instant.now().toString(),
                            "migration-method", "in-place-migrate"))
                    .execute();

            // 5. Post-Migration Validation
            if (!config.skipValidation()) {
                LOG.info("[{}] Performing post-migration count check...", tableName);
                long postCount = spark.table(tableName).count();
                if (preCount != postCount) {
                    LOG.error(
                            "[{}] Count Mismatch (Pre: {}, Post: {}). Rolling back...", tableName, preCount, postCount);
                    performRollback(spark, tableName, backupName); // Note: rollback uses simple name logic usually
                    return new MigrationResult(
                            tableName,
                            MigrationStatus.FAILURE,
                            System.currentTimeMillis() - start,
                            "Validation Failed: Data mismatch. Rolled back.");
                }
            }

            // 6. Cleanup Backup (Optional)
            String cleanupMsg = "";
            if (config.dropBackup()) {
                boolean dropped = safeDrop(spark, fullBackupName);
                cleanupMsg = dropped ? " (Backup Dropped)" : " (Backup Drop Failed)";
            } else {
                cleanupMsg = " (Backup: " + backupName + ")";
            }

            LOG.info("[{}] Migration successful. Format: {}", tableName, meta.format());
            return new MigrationResult(
                    tableName,
                    MigrationStatus.SUCCESS,
                    System.currentTimeMillis() - start,
                    "Success. Format: " + meta.format() + cleanupMsg);

        } catch (Exception e) {
            LOG.error("[{}] Migration Failed", tableName, e);
            // Attempt rollback if the original table is gone but backup exists
            if (!spark.catalog().tableExists(tableName) && spark.catalog().tableExists(fullBackupName)) {
                performRollback(spark, tableName, backupName); // Uses simple backup name if passed to SQL inside
                return new MigrationResult(
                        tableName,
                        MigrationStatus.FAILURE,
                        System.currentTimeMillis() - start,
                        "Error: " + e.getMessage() + " (Rolled Back)");
            }
            return new MigrationResult(
                    tableName, MigrationStatus.FAILURE, System.currentTimeMillis() - start, "Error: " + e.getMessage());
        }
    }

    /**
     * Analyzes table metadata in a single pass using `DESCRIBE EXTENDED`.
     * This reduces Metastore load by fetching format, type, and provider in one call.
     *
     * @param spark     The SparkSession.
     * @param tableName The table to analyze.
     * @return A TableMetadata record containing cached properties.
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

                // Using Enhanced Switch (Switch Expressions) for cleaner logic
                switch (key) {
                    case "InputFormat" -> {
                        String lower = val.toLowerCase();
                        if (lower.contains("iceberg")) isIceberg = true;
                        else if (lower.contains("parquet")) format = "parquet";
                        else if (lower.contains("orc")) format = "orc";
                        else if (lower.contains("avro")) format = "avro";
                        else format = val;
                    }
                    case "Provider" -> {
                        if (val.equalsIgnoreCase("iceberg")) isIceberg = true;
                    }
                    case "Type" -> {
                        if (val.toUpperCase().contains("MANAGED")) isManaged = true;
                    }
                    case "Location" -> location = val;
                    case "Table Properties" -> {
                        if (val != null && val.toUpperCase().contains("TABLE_TYPE=ICEBERG")) {
                            isIceberg = true;
                        }
                    }
                }
            }

            return new TableMetadata(true, isManaged, isIceberg, format, location);

        } catch (Exception e) {
            LOG.warn("Failed to analyze table {}", tableName, e);
            return new TableMetadata(false, false, false, "error", "");
        }
    }

    /**
     * Rolls back a failed migration by renaming the backup table back to the original name.
     * WARNING: Drops the current (potentially broken) table before renaming.
     */
    private void performRollback(SparkSession spark, String originalName, String backupName) {
        try {
            // Need fully qualified name for backup if original is qualified
            String qualifiedBackup = backupName;
            if (originalName.contains(".") && !backupName.contains(".")) {
                String db = originalName.substring(0, originalName.lastIndexOf("."));
                qualifiedBackup = db + "." + backupName;
            }

            LOG.warn("[{}] Attempting rollback using backup {}...", originalName, qualifiedBackup);
            spark.sql("DROP TABLE IF EXISTS " + originalName);
            if (spark.catalog().tableExists(qualifiedBackup)) {
                spark.sql(String.format("ALTER TABLE %s RENAME TO %s", qualifiedBackup, originalName));
                LOG.info("[{}] Rollback successful.", originalName);
            } else {
                LOG.error("[{}] Backup table {} not found. Cannot rollback.", originalName, qualifiedBackup);
            }
        } catch (Exception e) {
            LOG.error("[{}] Rollback failed.", originalName, e);
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
            if (!spark.catalog().tableExists(tableName)) return true;
            spark.sql(String.format("ALTER TABLE %s SET TBLPROPERTIES('EXTERNAL'='TRUE')", tableName));
            spark.sql("DROP TABLE " + tableName);
            return true;
        } catch (Exception e) {
            LOG.warn("Failed to drop backup " + tableName, e);
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
            LOG.info("[{}] Converted to EXTERNAL.", tableName);
        } catch (Exception e) {
            LOG.warn("[{}] Failed to set External status.", tableName, e);
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
     * Discovers tables based on input string (List, Single Table, or Database Scan).
     *
     * @param spark SparkSession
     * @param input Input string (comma-separated list, single table, or DB name)
     * @return List of fully qualified table names.
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
                    // Ignore backup tables to avoid infinite migration loops or accidental damage
                    .filter(t -> !t.name().endsWith("_hive_backup_"))
                    .map(t -> input + "." + t.name()) // We return fully qualified names here
                    .toList();
        } catch (Exception e) {
            LOG.error("Error scanning DB", e);
            return List.of();
        }
    }

    /**
     * Prints a formatted summary report of all migration tasks to the standard output.
     * <p>
     * This method iterates through the list of migration results and displays a tabular view
     * containing the Table Name, Migration Status, and any relevant messages (errors or success details).
     * It also calculates and prints aggregate statistics (Total, Success, Failed, Skipped).
     * </p>
     *
     * @param results The list of {@link MigrationResult} objects collected from the batch execution.
     */
    private void printReport(List<MigrationResult> results) {
        System.out.println("\n" + "=".repeat(100));
        System.out.println(" MIGRATION REPORT");
        System.out.println("=".repeat(100));

        // Header format matches row format for alignment
        System.out.printf("%-30s | %-10s | %s%n", "Table", "Status", "Message");
        System.out.println("-".repeat(100));

        // Print individual rows
        results.forEach(res -> System.out.printf("%-30s | %-10s | %s%n", res.tableName(), res.status(), res.message()));

        System.out.println("=".repeat(100));

        // Calculate aggregate statistics using stream filtering
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
