/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.MemorySchemaHistory;
import io.debezium.relational.history.SchemaHistory;

public abstract class AbstractSchemaHistorySnapshotTest {

    protected SchemaHistorySnapshot snapshot;
    protected SchemaHistory history;
    protected Map<String, Object> source1;
    protected Map<String, Object> source2;
    protected Tables s1_t0, s1_t1, s1_t2, s1_t3;
    protected Tables s2_t0, s2_t1, s2_t2, s2_t3;
    protected DdlParser parser;

    @Before
    public void beforeEach() {
        history = new MemorySchemaHistory();
        snapshot = createHistorySnapshot();

        source1 = server("abc");
        source2 = server("xyz");

        parser = new MySqlAntlrDdlParser();
        s1_t0 = new Tables();
        s1_t1 = new Tables();
        s1_t2 = new Tables();
        s1_t3 = new Tables();

        record(1, "CREATE TABLE foo ( first VARCHAR(22) NOT NULL );", s1_t3, s1_t2, s1_t1, s1_t0);
        record(23, "CREATE TABLE\nperson ( name VARCHAR(22) NOT NULL );", s1_t3, s1_t2, s1_t1);
        record(30, "CREATE TABLE address\n( street VARCHAR(22) NOT NULL );", s1_t3, s1_t2);
        record(32, "ALTER TABLE address ADD city VARCHAR(22) NOT NULL;", s1_t3);

        snapshot.save(source1, position(32), s1_t3);
        snapshot.save(source1, position(30), s1_t2);
        snapshot.save(source1, position(23), s1_t1);
        snapshot.save(source1, position(1), s1_t0);

        s2_t0 = new Tables();
        s2_t1 = new Tables();
        s2_t2 = new Tables();
        s2_t3 = new Tables();

        record(2, "CREATE TABLE foo1 ( first VARCHAR(22) NOT NULL );", s2_t3, s2_t2, s2_t1, s2_t0);
        record(12, "CREATE TABLE\nperson1 ( name VARCHAR(22) NOT NULL );", s2_t3, s2_t2, s2_t1);
        record(16, "CREATE TABLE address1\n( street VARCHAR(22) NOT NULL );", s2_t3, s2_t2);
        record(99, "ALTER TABLE address1 ADD city VARCHAR(22) NOT NULL;", s2_t3);

        snapshot.save(source2, position(55), s2_t3);
        snapshot.save(source2, position(16), s2_t2);
        snapshot.save(source2, position(12), s2_t1);
        snapshot.save(source2, position(2), s2_t0);
    }

    @After
    public void afterEach() {
    }

    protected abstract SchemaHistorySnapshot createHistorySnapshot();

    protected Map<String, Object> server(String serverName) {
        return Map.of("server", serverName);
    }

    protected Map<String, Object> position(long pos) {
        return Map.of("file", "mysql-bin-changelog.000011", "pos", pos);
    }

    protected void record(long pos, String ddl, Tables... update) {
        try {
            history.record(source1, position(pos), "db", ddl);
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

    @Test
    public void shouldRecordSnapshotsAndRecoverToClosest() {
        assertNull(snapshot.read(source1, null));
        assertNull(snapshot.read(null, position(31)));
        assertNull(snapshot.read(source1, position(31)));
        assertEquals(s1_t2, snapshot.read(source1, position(30)));

        assertNull(snapshot.read(source2, position(30)));
        assertEquals(s2_t1, snapshot.read(source2, position(12)));

        assertNull(snapshot.findClosest(null, position(31)));
        assertNull(snapshot.findClosest(source1, null));
        assertEquals(position(30), snapshot.findClosest(source1, position(31)));
        assertEquals(position(32), snapshot.findClosest(source1, position(99)));

        assertNull(snapshot.findClosest(source2, position(1)));
        assertEquals(position(55), snapshot.findClosest(source2, position(99)));
    }
}
