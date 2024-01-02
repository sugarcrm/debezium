/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.util.function.Consumer;

import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.Tables;

public interface HistoryRecordProcessorProvider {
    Consumer<HistoryRecord> get(Offsets<?, ?> offsets, Tables schema);

    HistoryRecordProcessorProvider NOOP = (o, s) -> (r) -> {};
}
