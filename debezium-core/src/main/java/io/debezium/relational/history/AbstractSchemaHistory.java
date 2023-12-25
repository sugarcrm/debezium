/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.function.Predicates;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.relational.history.TableChanges.TableChangeType;
import io.debezium.relational.history.TableChanges.TableChangesSerializer;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;
import io.debezium.util.Clock;

/**
 * @author Randall Hauch
 *
 */
public abstract class AbstractSchemaHistory implements SchemaHistory {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    public static Field.Set ALL_FIELDS = Field.setOf(NAME, INTERNAL_CONNECTOR_CLASS, INTERNAL_CONNECTOR_ID);

    protected Configuration config;
    private HistoryRecordComparator comparator = HistoryRecordComparator.INSTANCE;
    private SchemaHistoryListener listener = SchemaHistoryListener.NOOP;
    private boolean useCatalogBeforeSchema;

    protected AbstractSchemaHistory() {
    }

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        this.config = config;
        this.comparator = comparator != null ? comparator : HistoryRecordComparator.INSTANCE;

        this.listener = listener;
        this.useCatalogBeforeSchema = useCatalogBeforeSchema;
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
    public void recover(Map<Map<String, ?>, Map<String, ?>> offsets, Tables schema, DdlParser ddlParser) {
        listener.recoveryStarted();
        final HistoryRecordProcessor recordProcessor = new HistoryRecordProcessor(
                offsets, schema, ddlParser,
                config, comparator, listener,
                useCatalogBeforeSchema);
        recoverRecords(recordProcessor);
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
