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

import io.debezium.config.Configuration;
import io.debezium.document.Document;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Tables;
import io.debezium.relational.history.snapshot.SchemaHistorySnapshot;
import io.debezium.util.Clock;

public class SnapshotAwareSchemaHistory implements SchemaHistory {

    private final SchemaHistory delegate;
    private final SchemaHistorySnapshot snapshot;
    private Tables schemaToSave = null;

    public SnapshotAwareSchemaHistory(SchemaHistory delegate, SchemaHistorySnapshot snapshot) {
        this.delegate = delegate;
        this.snapshot = snapshot;
    }

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, HistoryRecordProcessorProvider processorProvider) {
        delegate.configure(config, comparator, listener, (o, s) -> new HistoryRecordProcessor(o, s, comparator, listener, snapshot, processorProvider));
    }

    @Override
    public void start() {
        delegate.start();
    }

    @Override
    public void record(Map<String, ?> source, Map<String, ?> position, String databaseName, String ddl) throws SchemaHistoryException {
        record(source, position, databaseName, null, ddl, null, Clock.SYSTEM.currentTimeAsInstant());
    }

    @Override
    public void record(Map<String, ?> source, Map<String, ?> position, String databaseName, String schemaName, String ddl, TableChanges changes, Instant timestamp)
            throws SchemaHistoryException {
        delegate.record(source, position, databaseName, schemaName, ddl, changes, timestamp);
        snapshot.save(source, position, schemaToSave);
    }

    @Override
    public void recover(Offsets<?, ?> offsets, Tables schema) {
        delegate.recover(offsets, schema);

        // once recovered the schema can be used to create snapshots
        schemaToSave = schema;
    }

    @Override
    public void stop() {
        delegate.stop();
    }

    @Override
    public boolean exists() {
        return delegate.exists();
    }

    @Override
    public boolean storageExists() {
        return delegate.storageExists();
    }

    @Override
    public void initializeStorage() {
        delegate.initializeStorage();
    }

    private static class HistoryRecordProcessor implements Consumer<HistoryRecord> {

        private final HistoryRecordComparator comparator;
        private final SchemaHistoryListener listener;
        private final SchemaHistorySnapshot snapshot;
        private final Consumer<HistoryRecord> originalProcessor;
        private final Map<Document, HistoryRecord> stopPoints;

        public HistoryRecordProcessor(Offsets<?, ?> offsets, Tables schema, HistoryRecordComparator comparator, SchemaHistoryListener listener, SchemaHistorySnapshot snapshot, HistoryRecordProcessorProvider processorProvider) {
            this.comparator = comparator;
            this.listener = listener;
            this.snapshot = snapshot;
            this.stopPoints = getStopPoints(offsets, schema);
            this.originalProcessor = processorProvider.get(offsets, schema);
        }

        private Map<Document, HistoryRecord> getStopPoints(Offsets<?, ?> offsets, Tables schema) {
            Map<Document, HistoryRecord> stopPoints = new HashMap<>();
            getSnapshotOffsets(offsets).forEach((Map<String, ?> source, Map<String, ?> position) -> {
                Tables schemaPartition = snapshot.read(source, position);
                if (schemaPartition != null) {
                    merge(schema, schemaPartition);
                }

                Document srcDocument = Document.create();
                if (source != null) {
                    source.forEach(srcDocument::set);
                }
                stopPoints.put(srcDocument, new HistoryRecord(source, position));
            });
            return stopPoints;
        }

        private void merge(Tables schema, Tables partialSchema) {
            partialSchema.tableIds().forEach((tableId) -> schema.overwriteTable(partialSchema.forTable(tableId)));
        }

        private Map<Map<String, ?>, Map<String, ?>> getSnapshotOffsets(Offsets<?, ?> offsets) {
            Map<Map<String, ?>, Map<String, ?>> snapshotOffsets = new HashMap<>();
            offsets.forEach((Map.Entry<? extends Partition, ? extends OffsetContext> entry) -> {
                Map<String, ?> source = entry.getKey().getSourcePartition();
                Map<String, ?> position = null;
                if (entry.getValue() != null) {
                    position = entry.getValue().getOffset();
                }
                snapshotOffsets.put(source, snapshot.findClosest(source, position));
            });
            return snapshotOffsets;
        }

        /**
         * The code ignores records covered by schema snapshot - from the beginning to snapshot
         * offsets (SO). Then it switches to the original processor which handles records from SO
         * to connector offsets (CO).
         *
         * <-------------->                   : snapshot processor
         * ---------------(SO)-----(CO)---->  : schema history
         *                   <----->          : original processor
         * @param recovered the recovered schema history record
         */
        @Override
        public void accept(HistoryRecord recovered) {
            listener.onChangeFromHistory(recovered);
            Document srcDocument = recovered.document().getDocument(HistoryRecord.Fields.SOURCE);
            if (stopPoints.containsKey(srcDocument) && !stopPoints.get(srcDocument).position().isEmpty() && comparator.isAtOrBefore(recovered, stopPoints.get(srcDocument))) {
                // schema snapshot contains this record, skip processing
                listener.onChangeApplied(recovered);
            }
            else {
                originalProcessor.accept(recovered);
            }
        }
    }
}
