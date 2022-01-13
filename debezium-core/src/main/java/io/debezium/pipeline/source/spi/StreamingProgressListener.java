/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.ChangeTable;

/**
 * A class invoked by {@link StreamingChangeEventSource} whenever an important event or change of state happens.
 *
 * @author Jacob Gminder
 */
public interface StreamingProgressListener {

    void onReadingNewChangeTable(Partition partition, ChangeTable table);
}
