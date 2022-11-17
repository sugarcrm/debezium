/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Carries error metrics.
 */
public class ErrorMeter implements ErrorListener {

    private final AtomicInteger retriableErrorCount = new AtomicInteger();

    public int getRetriableErrorCount() {
        return retriableErrorCount.get();
    }

    public void onRetriableError() {
        retriableErrorCount.incrementAndGet();
    }
}
