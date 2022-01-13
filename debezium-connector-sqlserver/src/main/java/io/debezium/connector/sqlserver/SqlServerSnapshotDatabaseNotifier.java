/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;

/**
 * A class invoked by {@link SnapshotChangeEventSource} to notify the database of events via stored procedures
 *
 * @author Jacob Gminder
 */
public class SqlServerSnapshotDatabaseNotifier implements SnapshotProgressListener {
    private final SqlServerConnection dataConnection;

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerSnapshotDatabaseNotifier.class);

    SqlServerSnapshotDatabaseNotifier(final SqlServerConnection dataConnection) {
        this.dataConnection = dataConnection;
    }

    @Override
    public void snapshotStarted(Partition partition) {
        SqlServerPartition sqlServerPartition = (SqlServerPartition) partition;
        LOGGER.info("Started snapshot of database: {}", sqlServerPartition.getDatabaseName());
        // TODO
    }

    @Override
    public void monitoredDataCollectionsDetermined(Partition partition, Iterable<? extends DataCollectionId> dataCollectionIds) {
        // Do nothing
    }

    @Override
    public void snapshotCompleted(Partition partition) {
        SqlServerPartition sqlServerPartition = (SqlServerPartition) partition;
        LOGGER.info("Completed snapshot of database: {}", sqlServerPartition.getDatabaseName());
        // TODO
    }

    @Override
    public void snapshotAborted(Partition partition) {
        SqlServerPartition sqlServerPartition = (SqlServerPartition) partition;
        LOGGER.info("Aborted snapshot of database: {}", sqlServerPartition.getDatabaseName());
        // TODO
    }

    @Override
    public void dataCollectionSnapshotCompleted(Partition partition, DataCollectionId dataCollectionId, long numRows) {
        SqlServerPartition sqlServerPartition = (SqlServerPartition) partition;
        LOGGER.info("Completed snapshot of database: {}, dataCollectionId: {}", sqlServerPartition.getDatabaseName(), dataCollectionId.identifier());
        // TODO
    }

    @Override
    public void rowsScanned(Partition partition, TableId tableId, long numRows) {
        // Do nothing
    }

    @Override
    public void currentChunk(Partition partition, String chunkId, Object[] chunkFrom, Object[] chunkTo) {
        // Do nothing
    }

    @Override
    public void currentChunk(Partition partition, String chunkId, Object[] chunkFrom, Object[] chunkTo, Object[] tableTo) {
        // Do nothing
    }
}
