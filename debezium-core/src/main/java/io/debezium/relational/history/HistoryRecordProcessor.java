/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import static io.debezium.relational.history.SchemaHistory.DDL_FILTER;
import static io.debezium.relational.history.SchemaHistory.INTERNAL_PREFER_DDL;
import static io.debezium.relational.history.SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.function.Predicates;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.TableChanges.TableChangesSerializer;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;

public class HistoryRecordProcessor implements Consumer<HistoryRecord> {

    protected final Logger logger = LoggerFactory.getLogger(HistoryRecordProcessor.class);

    private final Map<Document, HistoryRecord> stopPoints;
    private final Tables schema;
    private final DdlParser ddlParser;
    private final TableChangesSerializer<Array> tableChangesSerializer = new JsonTableChangeSerializer();
    private final Configuration config;
    private final HistoryRecordComparator comparator;
    private final SchemaHistoryListener listener;
    private final boolean useCatalogBeforeSchema;
    private final boolean skipUnparseableDDL;
    private final boolean preferDdl;
    private Predicate<String> ddlFilter = x -> false;

    public HistoryRecordProcessor(
            Map<Map<String, ?>, Map<String, ?>> offsets, Tables schema, DdlParser ddlParser,
            Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener,
            boolean useCatalogBeforeSchema) {
        this.stopPoints = getStopPoints(offsets);
        this.schema = schema;
        this.ddlParser = ddlParser;

        this.config = config;
        this.comparator = comparator != null ? comparator : HistoryRecordComparator.INSTANCE;
        this.listener = listener != null ? listener : SchemaHistoryListener.NOOP;

        this.useCatalogBeforeSchema = useCatalogBeforeSchema;
        this.skipUnparseableDDL = config.getBoolean(SKIP_UNPARSEABLE_DDL_STATEMENTS);
        this.preferDdl = config.getBoolean(INTERNAL_PREFER_DDL);

        final String ddlFilter = config.getString(DDL_FILTER);
        this.ddlFilter = (ddlFilter != null) ? Predicates.includes(ddlFilter, Pattern.CASE_INSENSITIVE | Pattern.DOTALL) : (x -> false);
    }

    private Map<Document, HistoryRecord> getStopPoints(Map<Map<String, ?>, Map<String, ?>> offsets) {
        Map<Document, HistoryRecord> stopPoints = new HashMap<>();
        offsets.forEach((Map<String, ?> source, Map<String, ?> position) -> {
            Document srcDocument = Document.create();
            if (source != null) {
                source.forEach(srcDocument::set);
            }
            stopPoints.put(srcDocument, new HistoryRecord(source, position, null, null, null, null, null));
        });
        return stopPoints;
    }

    @Override
    public void accept(HistoryRecord recovered) {
        listener.onChangeFromHistory(recovered);
        Document srcDocument = recovered.document().getDocument(HistoryRecord.Fields.SOURCE);
        if (stopPoints.containsKey(srcDocument) && comparator.isAtOrBefore(recovered, stopPoints.get(srcDocument))) {
            Array tableChanges = recovered.tableChanges();
            String ddl = recovered.ddl();

            if (!preferDdl && tableChanges != null && !tableChanges.isEmpty()) {
                TableChanges changes = tableChangesSerializer.deserialize(tableChanges, useCatalogBeforeSchema);
                for (TableChanges.TableChange entry : changes) {
                    if (entry.getType() == TableChanges.TableChangeType.CREATE) {
                        schema.overwriteTable(entry.getTable());
                    }
                    else if (entry.getType() == TableChanges.TableChangeType.ALTER) {
                        if (entry.getPreviousId() != null) {
                            schema.removeTable(entry.getPreviousId());
                        }
                        schema.overwriteTable(entry.getTable());
                    }
                    // DROP
                    else {
                        schema.removeTable(entry.getId());
                    }
                }
                listener.onChangeApplied(recovered);
            }
            else if (ddl != null && ddlParser != null) {
                if (recovered.databaseName() != null) {
                    ddlParser.setCurrentDatabase(recovered.databaseName()); // may be null
                }
                if (recovered.schemaName() != null) {
                    ddlParser.setCurrentSchema(recovered.schemaName()); // may be null
                }
                if (ddlFilter.test(ddl)) {
                    logger.info("a DDL '{}' was filtered out of processing by regular expression '{}'", ddl,
                            config.getString(DDL_FILTER));
                    return;
                }
                try {
                    logger.debug("Applying: {}", ddl);
                    ddlParser.parse(ddl, schema);
                    listener.onChangeApplied(recovered);
                }
                catch (final ParsingException | MultipleParsingExceptions e) {
                    if (skipUnparseableDDL) {
                        logger.warn("Ignoring unparseable statements '{}' stored in database schema history", ddl, e);
                    }
                    else {
                        throw e;
                    }
                }
            }
        }
        else {
            logger.debug("Skipping: {}", recovered.ddl());
        }
    }
}
