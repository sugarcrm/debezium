/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

/**
 * Exposes error metrics.
 */
public interface ErrorMetricsMXBean {

    int getRetriableErrorCount();
}
