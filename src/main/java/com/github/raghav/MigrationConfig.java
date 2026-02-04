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

import java.util.List;

/**
 * Immutable configuration record holding all runtime settings.
 *
 * @param tableList                List of fully qualified table names to process.
 * @param dropBackup               If true, drops the Hive backup table after a successful migration.
 * @param dryRun                   If true, simulates the migration checks without making changes.
 * @param convertManagedToExternal If true, automatically converts MANAGED tables to EXTERNAL.
 * @param skipValidation           If true, skips the expensive pre/post row count validation.
 * @param threadPoolSize           Number of concurrent migration threads.
 */
public record MigrationConfig(
        List<String> tableList,
        boolean dropBackup,
        boolean dryRun,
        boolean convertManagedToExternal,
        boolean skipValidation,
        int threadPoolSize) {}
