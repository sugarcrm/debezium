/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history.snapshot;

import java.util.Map;

import io.debezium.relational.Tables.TableFilter;

public interface SchemaPartitioner {
    TableFilter fromSource(Map<String, ?> source);

    SchemaPartitioner SINGLE_PARTITION = source -> TableFilter.includeAll();
}
