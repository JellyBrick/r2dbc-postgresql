/*
 * Copyright 2017 the original author or authors.
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

interface StatementCache {

    /**
     * Name of the unnamed statement. A query bound to it must be parsed in the same message batch.
     */
    String UNNAMED_STATEMENT_NAME = "";

    String getName(Binding binding, String sql);

    boolean requiresPrepare(Binding binding, String sql);

    void put(Binding binding, String sql, String name);

    void evict(String sql);

    /**
     * Create the statement cache for a connection.
     *
     * @param client                        the client used to close evicted statements
     * @param preparedStatementCacheQueries maximum number of cached queries, {@code -1} for no limit, {@code 0} disables the cache
     * @param preparedStatementCacheSizeMiB maximum estimated size of cached queries in MiB, {@code -1} for no limit, {@code 0} disables the cache
     * @param prepareThreshold              executions of the same query before it is prepared on the server, {@code 0} never prepares
     * @return the statement cache
     */
    static StatementCache create(Client client, int preparedStatementCacheQueries, int preparedStatementCacheSizeMiB, int prepareThreshold) {
        if (preparedStatementCacheQueries == 0 || preparedStatementCacheSizeMiB == 0 || prepareThreshold == 0) {
            return new DisabledStatementCache();
        }

        int maxQueries = preparedStatementCacheQueries < 0 ? Integer.MAX_VALUE : preparedStatementCacheQueries;
        long maxSizeBytes = preparedStatementCacheSizeMiB < 0 ? Long.MAX_VALUE : preparedStatementCacheSizeMiB * 1024L * 1024L;

        StatementCache cache = preparedStatementCacheQueries < 0 && preparedStatementCacheSizeMiB < 0
            ? new IndefiniteStatementCache()
            : new BoundedStatementCache(client, maxQueries, maxSizeBytes);

        return prepareThreshold > 1 ? new ThresholdStatementCache(cache, prepareThreshold, maxQueries, maxSizeBytes) : cache;
    }

}
