/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.postgresql;

import io.r2dbc.postgresql.BoundedStatementCache.CacheKey;
import io.r2dbc.postgresql.client.Binding;
import io.r2dbc.postgresql.util.Assert;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@link StatementCache} that runs a query as an unnamed statement until it has been executed {@code prepareThreshold} times and only then lets the delegate prepare it
 * on the server, like pgjdbc's {@code prepareThreshold}. Queries that are not repeated, such as an {@code IN} list whose length keeps changing, therefore never hold a
 * server-side prepared statement.
 * <p>
 * Execution counts are kept for the least recently used queries within the same count and size limits as the delegate cache.
 */
final class ThresholdStatementCache implements StatementCache {

    private final StatementCache delegate;

    private final int prepareThreshold;

    private final int maxTrackedQueries;

    private final long maxTrackedSizeBytes;

    private final Map<CacheKey, Integer> executions = new LinkedHashMap<>(16, 0.75f, true);

    private long trackedSizeBytes;

    ThresholdStatementCache(StatementCache delegate, int prepareThreshold, int maxTrackedQueries, long maxTrackedSizeBytes) {
        this.delegate = Assert.requireNonNull(delegate, "delegate must not be null");
        Assert.isTrue(prepareThreshold > 1, "prepareThreshold must be greater than one");
        Assert.isTrue(maxTrackedQueries > 0, "maxTrackedQueries must be greater than zero");
        Assert.isTrue(maxTrackedSizeBytes > 0, "maxTrackedSizeBytes must be greater than zero");
        this.prepareThreshold = prepareThreshold;
        this.maxTrackedQueries = maxTrackedQueries;
        this.maxTrackedSizeBytes = maxTrackedSizeBytes;
    }

    @Override
    public String getName(Binding binding, String sql) {
        Assert.requireNonNull(binding, "binding must not be null");
        Assert.requireNonNull(sql, "sql must not be null");

        if (!this.delegate.requiresPrepare(binding, sql) || getExecutions(new CacheKey(sql, binding.getParameterTypes())) >= this.prepareThreshold) {
            return this.delegate.getName(binding, sql);
        }

        return UNNAMED_STATEMENT_NAME;
    }

    @Override
    public boolean requiresPrepare(Binding binding, String sql) {
        Assert.requireNonNull(binding, "binding must not be null");
        Assert.requireNonNull(sql, "sql must not be null");

        if (!this.delegate.requiresPrepare(binding, sql)) {
            return false;
        }

        recordExecution(new CacheKey(sql, binding.getParameterTypes()));
        return true;
    }

    @Override
    public void put(Binding binding, String sql, String name) {
        if (!UNNAMED_STATEMENT_NAME.equals(name)) {
            this.delegate.put(binding, sql, name);
        }
    }

    @Override
    public void evict(String sql) {
        this.delegate.evict(sql);
    }

    /**
     * Synchronized cache access: Return the execution count of a query.
     *
     * @param key Cache key
     * @return the number of recorded executions
     */
    int getExecutions(CacheKey key) {
        synchronized (this.executions) {
            Integer count = this.executions.get(key);
            return count != null ? count : 0;
        }
    }

    /**
     * Synchronized cache access: Count one execution and drop the least recently used counts until both limits hold again.
     */
    private void recordExecution(CacheKey key) {

        if (!BoundedStatementCache.fitsInCache(key.sql, this.maxTrackedSizeBytes)) {
            return;
        }

        synchronized (this.executions) {
            Integer count = this.executions.get(key);
            if (count == null) {
                this.trackedSizeBytes += BoundedStatementCache.estimateSize(key.sql);
                this.executions.put(key, 1);
            } else if (count < this.prepareThreshold) {
                this.executions.put(key, count + 1);
            }

            Iterator<CacheKey> iterator = this.executions.keySet().iterator();
            while ((this.executions.size() > this.maxTrackedQueries || this.trackedSizeBytes > this.maxTrackedSizeBytes) && iterator.hasNext()) {
                CacheKey eldest = iterator.next();
                iterator.remove();
                this.trackedSizeBytes -= BoundedStatementCache.estimateSize(eldest.sql);
            }
        }
    }

    @Override
    public String toString() {
        return "ThresholdStatementCache{" +
            "delegate=" + this.delegate +
            ", prepareThreshold=" + this.prepareThreshold +
            ", maxTrackedQueries=" + this.maxTrackedQueries +
            ", maxTrackedSizeBytes=" + this.maxTrackedSizeBytes +
            '}';
    }

}
