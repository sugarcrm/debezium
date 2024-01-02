/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.time.Instant;
import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.Tables;
import io.debezium.util.Clock;

/**
 * @author Randall Hauch
 *
 */
public abstract class AbstractSchemaHistory implements SchemaHistory {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected Configuration config;
    private SchemaHistoryListener listener = SchemaHistoryListener.NOOP;
    private HistoryRecordProcessorProvider processorProvider = HistoryRecordProcessorProvider.NOOP;

    protected AbstractSchemaHistory() {
    }

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, HistoryRecordProcessorProvider processorProvider) {
        this.config = config;
        this.listener = listener;
        this.processorProvider = processorProvider;
    }

    @Override
    public void start() {
        listener.started();
    }

    @Override
    public final void record(Map<String, ?> source, Map<String, ?> position, String databaseName, String ddl)
            throws SchemaHistoryException {

        record(source, position, databaseName, null, ddl, null, Clock.SYSTEM.currentTimeAsInstant());
    }

    @Override
    public final void record(Map<String, ?> source, Map<String, ?> position, String databaseName, String schemaName,
                             String ddl, TableChanges changes, Instant timestamp)
            throws SchemaHistoryException {
        final HistoryRecord record = new HistoryRecord(source, position, databaseName, schemaName, ddl, changes, timestamp);
        storeRecord(record);
        listener.onChangeApplied(record);
    }

    @Override
    public void recover(Offsets<?, ?> offsets, Tables schema) {
        listener.recoveryStarted();
        recoverRecords(processorProvider.get(offsets, schema));
        listener.recoveryStopped();
    }

    protected abstract void storeRecord(HistoryRecord record) throws SchemaHistoryException;

    protected abstract void recoverRecords(Consumer<HistoryRecord> records);

    @Override
    public void stop() {
        listener.stopped();
    }

    @Override
    public void initializeStorage() {
    }
}
