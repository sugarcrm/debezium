/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history.snapshot;

public class MemorySchemaHistorySnapshotTest extends AbstractSchemaHistorySnapshotTest {
    protected SchemaHistorySnapshot createHistorySnapshot() {
        return new MemorySchemaHistorySnapshot();
    }
}
