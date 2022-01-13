/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.source.spi.StreamingProgressListener;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.ChangeTable;

/**
 * A class invoked by {@link StreamingChangeEventSource} to notify the database of events via stored procedures
 *
 * @author Jacob Gminder
 */
public class SqlServerStreamingDatabaseNotifier implements StreamingProgressListener {
    private final SqlServerConnection dataConnection;

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerStreamingDatabaseNotifier.class);
    private static final String START_READING_CHANGE_TABLE = "EXEC [#db].dbo.DebeziumSQLConnector_StartReadingFromCaptureInstance @CaptureInstanceName = N'%s'";

    SqlServerStreamingDatabaseNotifier(final SqlServerConnection dataConnection) {
        this.dataConnection = dataConnection;
    }

    public void onReadingNewChangeTable(Partition partition, ChangeTable table) {
        SqlServerPartition sqlServerPartition = (SqlServerPartition) partition;
        final String query = dataConnection.replaceDatabaseNamePlaceholder(START_READING_CHANGE_TABLE, sqlServerPartition.getDatabaseName());
        final String startReadingChangeTableStmt = String.format(query, table.getCaptureInstance());
        try {
            LOGGER.trace("Calling the StartReadingFromCaptureInstance stored procedure for database: {} with capture instance: {}", sqlServerPartition.getDatabaseName(),
                    table.getCaptureInstance());
            dataConnection.execute(startReadingChangeTableStmt);
        }
        catch (SQLException ex) {
            LOGGER.trace(ex.getMessage());
        }
    }
}
