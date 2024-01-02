/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc.history;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Types;
import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.connector.SnapshotRecord;
import io.debezium.config.Configuration;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.HistoryRecordProcessor;
import io.debezium.relational.history.HistoryRecordProcessorProvider;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.relational.history.SchemaHistoryMetrics;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.TableChanges;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

/**
 * @author Ismail simsek
 */
public class JdbcSchemaHistoryTest {

    protected SchemaHistory history;
    String dbFile = "/tmp/test.db";
    static String databaseName = "db";
    static String schemaName = "myschema";
    static String ddl = "CREATE TABLE foo ( first VARCHAR(22) NOT NULL );";
    static Map<String, String> source;
    static Map<String, ?> position;
    static TableId tableId;
    static Table table;
    static TableChanges tableChanges;
    static HistoryRecord historyRecord;
    static Map<String, ?> position2;
    static TableId tableId2;
    static Table table2;
    static TableChanges tableChanges2;
    static HistoryRecord historyRecord2;
    static DdlParser ddlParser = new TestingAntlrDdlParser();
    static Instant currentInstant = Instant.now();

    @BeforeClass
    public static void beforeClass() {
        source = Collect.linkMapOf("server", "abc");
        position = Collect.linkMapOf("file", "x.log", "positionInt", 100, "positionLong", Long.MAX_VALUE, "entry", 1);
        tableId = new TableId(databaseName, schemaName, "foo");
        table = Table.editor()
                .tableId(tableId)
                .addColumn(Column.editor()
                        .name("first")
                        .jdbcType(Types.VARCHAR)
                        .type("VARCHAR")
                        .length(22)
                        .optional(false)
                        .create())
                .setPrimaryKeyNames("first")
                .create();
        tableChanges = new TableChanges().create(table);
        historyRecord = new HistoryRecord(source, position, databaseName, schemaName, ddl, tableChanges, currentInstant);
        //
        position2 = Collect.linkMapOf("file", "x.log", "positionInt", 100, "positionLong", Long.MAX_VALUE, "entry", 2);
        tableId2 = new TableId(databaseName, schemaName, "bar");
        table2 = Table.editor()
                .tableId(tableId2)
                .addColumn(Column.editor()
                        .name("first")
                        .jdbcType(Types.VARCHAR)
                        .type("VARCHAR")
                        .length(22)
                        .optional(false)
                        .create())
                .setPrimaryKeyNames("first")
                .create();
        tableChanges2 = new TableChanges().create(table2);
        historyRecord2 = new HistoryRecord(source, position, databaseName, schemaName, ddl, tableChanges2, currentInstant);
    }

    @Before
    public void beforeEach() {
        history = new JdbcSchemaHistory();

        Configuration config = Configuration.create()
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_JDBC_URL.name(), "jdbc:sqlite:" + dbFile)
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_USER.name(), "user")
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_PASSWORD.name(), "pass")
                .build();
        HistoryRecordComparator comparator = null;
        SchemaHistoryListener listener = SchemaHistoryMetrics.NOOP;
        HistoryRecordProcessorProvider processorProvider = (o, s) -> new HistoryRecordProcessor(o, s, ddlParser, config, comparator, listener, true);

        history.configure(config, comparator, listener, processorProvider);
        history.start();
    }

    @After
    public void afterEach() throws IOException {
        if (history != null) {
            history.stop();
        }
        Files.delete(Paths.get(dbFile));
    }

    protected Offsets<?, ?> offsets(Map<String, String> s, Map<String, ?> p) {
        return Offsets.of(() -> s, new TestOffsetContext(p));
    }

    @Test
    public void shouldSplitDatabaseAndTableName() {
        JdbcSchemaHistory schemaHistory = new JdbcSchemaHistory();

        Configuration config = Configuration.create()
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_JDBC_URL.name(), "jdbc:sqlite:" + dbFile)
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_USER.name(), "user")
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_PASSWORD.name(), "pass")
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_TABLE_NAME.name(), "public.employee")
                .build();
        HistoryRecordComparator comparator = null;
        SchemaHistoryListener listener = SchemaHistoryMetrics.NOOP;
        HistoryRecordProcessorProvider processorProvider = (o, s) -> new HistoryRecordProcessor(o, s, ddlParser, config, comparator, listener, true);

        schemaHistory.configure(config, comparator, listener, processorProvider);

        assertTrue(schemaHistory.getConfig().getDatabaseName().equalsIgnoreCase("public"));
        assertTrue(schemaHistory.getConfig().getTableName().equalsIgnoreCase("employee"));
    }

    @Test
    public void shouldNotFailMultipleInitializeStorage() {
        history.initializeStorage();
        history.initializeStorage();
        history.initializeStorage();
        assertTrue(history.storageExists());
        assertFalse(history.exists());
    }

    @Test
    public void shouldRecordChangesAndRecover() throws InterruptedException {
        history.record(source, position, databaseName, schemaName, ddl, tableChanges, currentInstant);
        history.record(source, position, databaseName, schemaName, ddl, tableChanges, currentInstant);
        Tables tables = new Tables();
        history.recover(offsets(source, position), tables);
        assertEquals(tables.size(), 1);
        assertEquals(tables.forTable(tableId), table);
        history.record(source, position2, databaseName, schemaName, ddl, tableChanges2, currentInstant);
        history.record(source, position2, databaseName, schemaName, ddl, tableChanges2, currentInstant);
        history.stop();
        // after restart, it should recover history correctly
        JdbcSchemaHistory history2 = new JdbcSchemaHistory();

        Configuration config = Configuration.create()
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_JDBC_URL.name(), "jdbc:sqlite:" + dbFile)
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_USER.name(), "user")
                .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + JdbcSchemaHistoryConfig.PROP_PASSWORD.name(), "pass")
                .build();
        HistoryRecordComparator comparator = null;
        SchemaHistoryListener listener = SchemaHistoryMetrics.NOOP;
        HistoryRecordProcessorProvider processorProvider = (o, s) -> new HistoryRecordProcessor(o, s, ddlParser, config, comparator, listener, true);

        history2.configure(config, comparator, listener, processorProvider);

        history2.start();
        assertTrue(history2.storageExists());
        assertTrue(history2.exists());
        Tables tables2 = new Tables();
        history2.recover(offsets(source, position2), tables2);
        assertEquals(tables2.size(), 2);
        assertEquals(tables2.forTable(tableId2), table2);
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
