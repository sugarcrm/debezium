/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.metrics.Metrics;

/**
 * Error metrics.
 */
public class ErrorMetrics extends Metrics implements ErrorMetricsMXBean {

    private final ErrorMeter errorMeter;

    public ErrorMetrics(CdcSourceTaskContext taskContext, ErrorMeter errorMeter) {
        super(taskContext);
        this.errorMeter = errorMeter;
    }

    @Override
    public int getRetriableErrorCount() {
        return errorMeter.getRetriableErrorCount();
    }
}
