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
import io.debezium.relational.history.SchemaHistory;

public interface SchemaHistorySnapshot {

    String CONFIGURATION_FIELD_PREFIX_STRING = SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + "snapshot.";

    void configure(Configuration config, HistoryRecordComparator comparator, SchemaPartitioner schemaPartitioner);

    void save(Map<String, ?> source, Map<String, ?> position, Tables schema);

    Tables read(Map<String, ?> source, Map<String, ?> position);

    Map<String, ?> findClosest(Map<String, ?> source, Map<String, ?> position);
}
