/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history.snapshot;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import io.debezium.relational.Tables;

public class MemorySchemaHistorySnapshot extends AbstractSchemaHistorySnapshot {

    private final Map<Map<Map<String, ?>, Map<String, ?>>, Tables> snapshots = new HashMap<>();

    public MemorySchemaHistorySnapshot() {
    }

    @Override
    public void doSave(Map<String, ?> source, Map<String, ?> position, Tables schema) {
        snapshots.put(Map.of(source, position), schema);
    }

    @Override
    public Tables doRead(Map<String, ?> source, Map<String, ?> position) {
        return snapshots.get(Map.of(source, position));
    }

    @Override
    protected List<Map<String, ?>> findAll(Map<String, ?> source) {
        return snapshots.keySet().stream()
                .map((e) -> e.get(source))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
