package io.debezium.relational.history;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.SnapshotRecord;
import io.debezium.config.Configuration;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.Tables;
import io.debezium.relational.history.snapshot.MemorySchemaHistorySnapshot;
import io.debezium.relational.history.snapshot.SchemaHistorySnapshot;
import io.debezium.relational.history.snapshot.SchemaPartitioner;
import io.debezium.spi.schema.DataCollectionId;

public class SnapshotAwareSchemaHistoryTest {

    private SchemaHistorySnapshot historySnapshot;
    private SchemaHistory delegate;
    private SnapshotAwareSchemaHistory schemaHistory;
    private final List<HistoryRecord> recovered = new ArrayList<>();
    private final static Map<String, String> SOURCE = Map.of("server", "dbserver1");

    @Before
    public void beforeEach() {
        HistoryRecordComparator comparator = HistoryRecordComparator.INSTANCE;
        historySnapshot = new MemorySchemaHistorySnapshot();
        historySnapshot.configure(Configuration.empty(), comparator, SchemaPartitioner.SINGLE_PARTITION);

        delegate = new MemorySchemaHistory();

        schemaHistory = new SnapshotAwareSchemaHistory(delegate, historySnapshot);
        schemaHistory.configure(Configuration.empty(), comparator, SchemaHistoryListener.NOOP, (o, s) -> recovered::add);

        recovered.clear();
    }

    @After
    public void afterEach() {
    }

    protected Partition partition() {
        return () -> SOURCE;
    }

    protected Map<String, ?> contextOffset(long pos) {
        return Map.of("pos", pos);
    }

    protected OffsetContext position(long pos) {
        return new TestOffsetContext(contextOffset(pos));
    }

    protected Offsets<?, ?> offsets(long pos) {
        return Offsets.of(partition(), position(pos));
    }

    protected void record(long pos, String ddl) {
        try {
            delegate.record(SOURCE, contextOffset(pos), "db", ddl);
        }
        catch (Throwable t) {
            fail(t.getMessage());
        }
    }

    @Test
    public void shouldSkipRecordsRecoveredFromSnapshot() {
        delegate.record(SOURCE, contextOffset(1), "db", "CREATE TABLE foo ( first VARCHAR(22) NOT NULL );");
        delegate.record(SOURCE, contextOffset(3), "db", "CREATE TABLE\nperson ( name VARCHAR(22) NOT NULL );");
        delegate.record(SOURCE, contextOffset(5), "db", "CREATE TABLE address\n( street VARCHAR(22) NOT NULL );");
        delegate.record(SOURCE, contextOffset(10), "db", "ALTER TABLE address ADD city VARCHAR(22) NOT NULL;");
        delegate.record(SOURCE, contextOffset(15), "db", "ALTER TABLE address ADD zip VARCHAR(22) NOT NULL;");

        historySnapshot.save(SOURCE, contextOffset(4), new Tables());
        historySnapshot.save(SOURCE, contextOffset(8), new Tables());

        schemaHistory.recover(offsets(1), new Tables());
        assertEquals(5, recovered.size());
        recovered.clear();

        schemaHistory.recover(offsets(4), new Tables());
        assertEquals(3, recovered.size());
        recovered.clear();

        schemaHistory.recover(offsets(6), new Tables());
        assertEquals(3, recovered.size());
        recovered.clear();

        schemaHistory.recover(offsets(9), new Tables());
        assertEquals(2, recovered.size());
        recovered.clear();

        schemaHistory.recover(offsets(20), new Tables());
        assertEquals(2, recovered.size());
        recovered.clear();
    }

    private static class TestOffsetContext implements OffsetContext {
        private final Map<String, ?> offset;

        TestOffsetContext(Map<String, ?> offset) {
            this.offset = offset;
        }

        @Override
        public Map<String, ?> getOffset() {
            return offset;
        }

        @Override
        public Schema getSourceInfoSchema() {
            return null;
        }

        @Override
        public Struct getSourceInfo() {
            return null;
        }

        @Override
        public boolean isSnapshotRunning() {
            return false;
        }

        @Override
        public void markSnapshotRecord(SnapshotRecord record) {

        }

        @Override
        public void preSnapshotStart() {

        }

        @Override
        public void preSnapshotCompletion() {

        }

        @Override
        public void postSnapshotCompletion() {

        }

        @Override
        public void event(DataCollectionId collectionId, Instant timestamp) {

        }

        @Override
        public TransactionContext getTransactionContext() {
            return null;
        }
    }
}
