/*
 * Copyright 2019 the original author or authors.
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

import io.r2dbc.postgresql.client.Binding;
import io.r2dbc.postgresql.client.Client;
import io.r2dbc.postgresql.client.ExtendedQueryMessageFlow;
import io.r2dbc.postgresql.util.Assert;
import org.jspecify.annotations.Nullable;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bounded {@link StatementCache} limited by the number of cached queries and by their estimated size. The least recently used statements are closed on the server
 * once either limit is exceeded. Queries too large to ever fit run as unnamed statements.
 */
final class BoundedStatementCache implements StatementCache {

    private static final Logger LOGGER = Loggers.getLogger(BoundedStatementCache.class);

    private final Map<CacheKey, String> cache = new LinkedHashMap<>(16, 0.75f, true);

    private final Client client;

    private final AtomicInteger counter = new AtomicInteger();

    private final int limit;

    private final long maxSizeBytes;

    private long currentSizeBytes;

    public BoundedStatementCache(Client client, int limit) {
        this(client, limit, Long.MAX_VALUE);
    }

    public BoundedStatementCache(Client client, int limit, long maxSizeBytes) {
        this.client = Assert.requireNonNull(client, "client must not be null");
        if (limit <= 0) {
            throw new IllegalArgumentException("statement cache limit must be greater than zero");
        }
        if (maxSizeBytes <= 0) {
            throw new IllegalArgumentException("statement cache size must be greater than zero");
        }
        this.limit = limit;
        this.maxSizeBytes = maxSizeBytes;
    }

    /**
     * Estimate the memory a cached query costs: two bytes per character for both the original and the native
     * SQL, plus the cache entry itself.
     *
     * @param sql the query
     * @return estimated size in bytes
     */
    static long estimateSize(String sql) {
        return sql.length() * 4L + 100;
    }

    /**
     * refuses entries above half of its budget so one query cannot flush the whole cache.
     */
    static boolean fitsInCache(String sql, long maxSizeBytes) {
        return estimateSize(sql) <= maxSizeBytes / 2;
    }

    @Override
    public String getName(Binding binding, String sql) {
        Assert.requireNonNull(binding, "binding must not be null");
        Assert.requireNonNull(sql, "sql must not be null");

        if (!fitsInCache(sql, this.maxSizeBytes)) {
            return UNNAMED_STATEMENT_NAME;
        }

        String name = get(new CacheKey(sql, binding.getParameterTypes()));

        if (name != null) {
            return name;
        }

        return "S_" + this.counter.getAndIncrement();
    }

    @Override
    public boolean requiresPrepare(Binding binding, String sql) {

        Assert.requireNonNull(binding, "binding must not be null");
        Assert.requireNonNull(sql, "sql must not be null");

        return !fitsInCache(sql, this.maxSizeBytes) || get(new CacheKey(sql, binding.getParameterTypes())) == null;
    }

    @Override
    public void put(Binding binding, String sql, String name) {

        if (UNNAMED_STATEMENT_NAME.equals(name)) {
            return;
        }

        CacheKey key = new CacheKey(sql, binding.getParameterTypes());

        for (Map.Entry<CacheKey, String> evicted : putAndEvictOverflow(key, name)) {
            close(evicted, ExceptionFactory.withSql(evicted.getKey().sql), evicted.getValue());
        }
    }

    @Override
    public void evict(String name) {

        synchronized (this.cache) {

            List<CacheKey> toRemove = new ArrayList<>();
            for (Map.Entry<CacheKey, String> entry : this.cache.entrySet()) {
                if (entry.getKey().sql.equals(name)) {
                    toRemove.add(entry.getKey());
                }
            }

            for (CacheKey key : toRemove) {
                this.cache.remove(key);
                this.currentSizeBytes -= estimateSize(key.sql);
            }
        }
    }

    private void close(Map.Entry<CacheKey, String> lastAccessedStatement, ExceptionFactory factory, String statementName) {
        ExtendedQueryMessageFlow
            .closeStatement(this.client, statementName)
            .handle(factory::handleErrorResponse)
            .subscribe(it -> {
            }, err -> LOGGER.warn(String.format("Cannot close statement %s (%s)", lastAccessedStatement.getValue(), lastAccessedStatement.getKey().sql), err));
    }

    /**
     * Synchronized cache access: Return all statement names.
     *
     * @return statement names.
     */
    Collection<String> getCachedStatementNames() {
        synchronized (this.cache) {
            List<String> names = new ArrayList<>(this.cache.size());
            names.addAll(this.cache.values());
            return names;
        }
    }

    /**
     * Synchronized cache access: Retrieve statement name by key.
     *
     * @param key Cache key
     * @see CacheKey
     * @return statement name by key
     */
    private @Nullable String get(CacheKey key) {
        synchronized (this.cache) {
            return this.cache.get(key);
        }
    }

    /**
     * Synchronized cache access: Store prepared statement and remove least recently used entries until both limits hold again.
     *
     * @return removed entries whose statements must be closed
     */
    private List<Map.Entry<CacheKey, String>> putAndEvictOverflow(CacheKey key, String preparedName) {
        synchronized (this.cache) {
            if (this.cache.put(key, preparedName) == null) {
                this.currentSizeBytes += estimateSize(key.sql);
            }

            List<Map.Entry<CacheKey, String>> evicted = new ArrayList<>();
            Iterator<Map.Entry<CacheKey, String>> iterator = this.cache.entrySet().iterator();
            while ((this.cache.size() > this.limit || this.currentSizeBytes > this.maxSizeBytes) && iterator.hasNext()) {
                Map.Entry<CacheKey, String> eldest = iterator.next();
                iterator.remove();
                this.currentSizeBytes -= estimateSize(eldest.getKey().sql);
                evicted.add(eldest);
            }
            return evicted;
        }
    }

    /**
     * Synchronized cache access: Return the estimated size of all cached queries.
     *
     * @return estimated size in bytes
     */
    long getCurrentSizeBytes() {
        synchronized (this.cache) {
            return this.currentSizeBytes;
        }
    }

    @Override
    public String toString() {
        return "LimitedStatementCache{" +
            "cache=" + this.cache +
            ", counter=" + this.counter +
            ", client=" + this.client +
            ", limit=" + this.limit +
            ", maxSizeBytes=" + this.maxSizeBytes +
            ", currentSizeBytes=" + this.currentSizeBytes +
            '}';
    }

    static class CacheKey {

        String sql;

        int[] parameterTypes;

        public CacheKey(String sql, int[] parameterTypes) {
            this.sql = sql;
            this.parameterTypes = parameterTypes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CacheKey)) {
                return false;
            }

            CacheKey cacheKey = (CacheKey) o;

            if (this.sql != null ? !this.sql.equals(cacheKey.sql) : cacheKey.sql != null) {
                return false;
            }
            return Arrays.equals(this.parameterTypes, cacheKey.parameterTypes);
        }

        @Override
        public int hashCode() {
            int result = this.sql != null ? this.sql.hashCode() : 0;
            result = 31 * result + Arrays.hashCode(this.parameterTypes);
            return result;
        }

    }

}
