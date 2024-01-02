/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.util.Map;
import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.connector.mysql.SourceInfo;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 *
 */
public abstract class AbstractSchemaHistoryTest {

    private static String SERVER_NAME = "server1";
    private static String BINLOG_FILE = "a.log";

    protected SchemaHistory history;
    protected Tables t0, t1, t2, t3, t4, all;
    protected DdlParser parser;

    @Before
    public void beforeEach() {
        parser = new MySqlAntlrDdlParser();
        t0 = new Tables();
        t1 = new Tables();
        t2 = new Tables();
        t3 = new Tables();
        t4 = new Tables();
        all = new Tables();
        history = createHistory();
    }

    @After
    public void afterEach() {
        if (history != null) {
            history.stop();
        }
    }

    protected abstract SchemaHistory createHistory();

    protected MySqlPartition source(String server) {
        return new MySqlPartition(server, "");
    }

    protected Map<String, Object> position(long position, int row) {
        return Collect.linkMapOf(
                SourceInfo.BINLOG_FILENAME_OFFSET_KEY, BINLOG_FILE,
                SourceInfo.BINLOG_POSITION_OFFSET_KEY, position,
                SourceInfo.BINLOG_ROW_IN_EVENT_OFFSET_KEY, row);
    }

    protected Offsets<?, ?> offsets(String server, long pos, int row) {
        return Offsets.of(source(server), new TestOffsetContext(position(pos, row)));
    }

    protected void record(long pos, int row, String ddl, Tables... update) {
        try {
            history.record(source(SERVER_NAME).getSourcePartition(), position(pos, row), "db", ddl);
        }
        catch (Throwable t) {
            fail(t.getMessage());
        }
        for (Tables tables : update) {
            if (tables != null) {
                parser.setCurrentSchema("db");
                parser.parse(ddl, tables);
            }
        }
    }

    protected Tables recover(long pos, int row) {
        Tables result = new Tables();
        history.recover(offsets(SERVER_NAME, pos, row), result);
        return result;
    }

    @Test
    public void shouldRecordChangesAndRecoverToVariousPoints() {
        record(1, 0,"CREATE TABLE foo ( first VARCHAR(22) NOT NULL );", all, t3, t2, t1, t0);
        record(23, 1, "CREATE TABLE\nperson ( name VARCHAR(22) NOT NULL );", all, t3, t2, t1);
        record(30, 2, "CREATE TABLE address\n( street VARCHAR(22) NOT NULL );", all, t3, t2);
        record(32, 3, "ALTER TABLE address ADD city VARCHAR(22) NOT NULL;", all, t3);

        // Testing.Print.enable();
        if (Testing.Print.isEnabled()) {
            Testing.print("t0 = " + t0);
            Testing.print("t1 = " + t1);
            Testing.print("t2 = " + t2);
            Testing.print("t3 = " + t3);
        }

        assertThat(recover(1, 0)).isEqualTo(t0);
        assertThat(recover(1, 3)).isEqualTo(t0);
        assertThat(recover(10, 1)).isEqualTo(t0);
        assertThat(recover(22, 999999)).isEqualTo(t0);
        assertThat(recover(23, 0)).isEqualTo(t0);

        assertThat(recover(23, 1)).isEqualTo(t1);
        assertThat(recover(23, 2)).isEqualTo(t1);
        assertThat(recover(23, 3)).isEqualTo(t1);
        assertThat(recover(29, 999)).isEqualTo(t1);
        assertThat(recover(30, 1)).isEqualTo(t1);

        assertThat(recover(30, 2)).isEqualTo(t2);
        assertThat(recover(30, 3)).isEqualTo(t2);
        assertThat(recover(32, 2)).isEqualTo(t2);

        assertThat(recover(32, 3)).isEqualTo(t3);
        assertThat(recover(32, 4)).isEqualTo(t3);
        assertThat(recover(33, 0)).isEqualTo(t3);
        assertThat(recover(33, 0)).isEqualTo(all);
        assertThat(recover(1033, 4)).isEqualTo(t3);
        assertThat(recover(1033, 4)).isEqualTo(t3);
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
