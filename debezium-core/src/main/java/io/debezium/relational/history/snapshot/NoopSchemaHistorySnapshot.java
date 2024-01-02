/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history.snapshot;

import java.util.Map;

import io.debezium.config.Configuration;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecordComparator;

public class NoopSchemaHistorySnapshot implements SchemaHistorySnapshot {

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaPartitioner schemaPartitioner) {

    }

    @Override
    public void save(Map<String, ?> source, Map<String, ?> position, Tables schema) {

    }

    @Override
    public Tables read(Map<String, ?> source, Map<String, ?> position) {
        return null;
    }

    @Override
    public Map<String, ?> findClosest(Map<String, ?> source, Map<String, ?> position) {
        return null;
    }
}
