/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import io.debezium.config.Configuration;

/**
 * @author Randall Hauch
 */
public class MemorySchemaHistoryTest extends AbstractSchemaHistoryTest {

    @Override
    protected SchemaHistory createHistory() {
        SchemaHistory history = new MemorySchemaHistory();

        Configuration config = Configuration.empty();
        HistoryRecordComparator comparator = null;
        SchemaHistoryListener listener = SchemaHistoryMetrics.NOOP;
        HistoryRecordProcessorProvider processorProvider = (o, s) -> new HistoryRecordProcessor(o, s, parser, config, comparator, listener, true);

        history.configure(config, comparator, listener, processorProvider);
        history.start();

        return history;
    }
}
