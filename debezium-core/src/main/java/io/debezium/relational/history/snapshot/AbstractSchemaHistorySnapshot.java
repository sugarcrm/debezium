/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history.snapshot;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.util.FunctionalReadWriteLock;

public abstract class AbstractSchemaHistorySnapshot implements SchemaHistorySnapshot {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected Configuration config;
    protected HistoryRecordComparator comparator = HistoryRecordComparator.INSTANCE;
    protected SchemaPartitioner schemaPartitioner = SchemaPartitioner.SINGLE_PARTITION;
    private final FunctionalReadWriteLock lock = FunctionalReadWriteLock.reentrant();

    protected AbstractSchemaHistorySnapshot() {
    }

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaPartitioner schemaPartitioner) {
        this.config = config;
        this.comparator = comparator;
        this.schemaPartitioner = schemaPartitioner;
    }

    @Override
    public void save(Map<String, ?> source, Map<String, ?> position, Tables schema) {
        if (source == null || position == null || schema == null) {
            return;
        }

        lock.write(() -> doSave(source, position, getSchemaPartition(source, schema)));
    }

    private Tables getSchemaPartition(Map<String, ?> source, Tables schema) {
        if (schemaPartitioner == null) {
            return schema;
        }
        return schema.subset(schemaPartitioner.fromSource(source));
    }

    @Override
    public Tables read(Map<String, ?> source, Map<String, ?> position) {
        if (source == null || position == null) {
            return null;
        }
        return lock.read(() -> doRead(source, position));
    }

    @Override
    public Map<String, ?> findClosest(Map<String, ?> source, Map<String, ?> position) {
        if (source == null || position == null) {
            return null;
        }
        HistoryRecord historyRecord = new HistoryRecord(source, position);
        return lock.read(() -> findAll(source))
                .stream()
                // snapshot position keys match given position keys
                .filter((p) -> p.keySet().equals(position.keySet()))
                // snapshot position is smaller than given position
                .filter((p) -> comparator.isAtOrBefore(new HistoryRecord(source, p), historyRecord))
                // sort positions ASC and get last
                .max((p1, p2) -> {
                    HistoryRecord h1 = new HistoryRecord(source, p1);
                    HistoryRecord h2 = new HistoryRecord(source, p2);
                    return comparator.isAtOrBefore(h1, h2) ? -1 : comparator.isAtOrBefore(h2, h1) ? 1 : 0;
                })
                .orElse(null);
    }

    protected abstract void doSave(Map<String,?> source, Map<String,?> position, Tables schema);

    protected abstract Tables doRead(Map<String, ?> source, Map<String, ?> position);

    protected abstract List<Map<String, ?>> findAll(Map<String, ?> source);
}
