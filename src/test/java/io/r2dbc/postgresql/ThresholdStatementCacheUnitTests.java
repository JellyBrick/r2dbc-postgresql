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
import io.r2dbc.postgresql.client.Client;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link ThresholdStatementCache}.
 */
class ThresholdStatementCacheUnitTests {

    @Test
    void runsUnnamedUntilThresholdThenPreparesOnce() {

        BoundedStatementCache delegate = new BoundedStatementCache(mock(Client.class), 100);
        ThresholdStatementCache cache = new ThresholdStatementCache(delegate, 3, 100, Long.MAX_VALUE);

        assertThat(execute(cache, "foo")).isEqualTo("");
        assertThat(execute(cache, "foo")).isEqualTo("");
        assertThat(delegate.getCachedStatementNames()).isEmpty();

        assertThat(cache.requiresPrepare(new Binding(0), "foo")).isTrue();
        assertThat(cache.getName(new Binding(0), "foo")).isEqualTo("S_0");
        cache.put(new Binding(0), "foo", "S_0");
        assertThat(delegate.getCachedStatementNames()).containsOnly("S_0");

        assertThat(cache.requiresPrepare(new Binding(0), "foo")).isFalse();
        assertThat(cache.getName(new Binding(0), "foo")).isEqualTo("S_0");
    }

    @Test
    void preparedQueryStaysNamedWhenItsCountIsDropped() {

        BoundedStatementCache delegate = new BoundedStatementCache(mock(Client.class), 100);
        ThresholdStatementCache cache = new ThresholdStatementCache(delegate, 2, 1, Long.MAX_VALUE);

        execute(cache, "foo");
        assertThat(execute(cache, "foo")).isEqualTo("S_0");

        execute(cache, "bar");
        assertThat(cache.getExecutions(new CacheKey("foo", new int[0]))).isZero();

        assertThat(cache.requiresPrepare(new Binding(0), "foo")).isFalse();
        assertThat(cache.getName(new Binding(0), "foo")).isEqualTo("S_0");
    }

    @Test
    void countsOfLeastRecentlyUsedQueriesAreDroppedAtTheQueryLimit() {

        ThresholdStatementCache cache = new ThresholdStatementCache(new BoundedStatementCache(mock(Client.class), 100), 5, 2, Long.MAX_VALUE);

        execute(cache, "foo");
        execute(cache, "foo");
        execute(cache, "bar");
        execute(cache, "baz");

        assertThat(cache.getExecutions(new CacheKey("foo", new int[0]))).isZero();
        assertThat(cache.getExecutions(new CacheKey("bar", new int[0]))).isOne();
        assertThat(cache.getExecutions(new CacheKey("baz", new int[0]))).isOne();
    }

    @Test
    void countsOfLeastRecentlyUsedQueriesAreDroppedAtTheSizeLimit() {

        // Each count is estimated at 112 bytes, so two fit into 250 bytes.
        ThresholdStatementCache cache = new ThresholdStatementCache(new BoundedStatementCache(mock(Client.class), 100), 5, 100, 250);

        execute(cache, "foo");
        execute(cache, "bar");
        execute(cache, "baz");

        assertThat(cache.getExecutions(new CacheKey("foo", new int[0]))).isZero();
        assertThat(cache.getExecutions(new CacheKey("baz", new int[0]))).isOne();
    }

    @Test
    void queryAboveHalfOfSizeLimitIsNeitherCountedNorPrepared() {

        BoundedStatementCache delegate = new BoundedStatementCache(mock(Client.class), 100, 223);
        ThresholdStatementCache cache = new ThresholdStatementCache(delegate, 2, 100, 223);

        for (int i = 0; i < 5; i++) {
            assertThat(execute(cache, "foo")).isEqualTo("");
        }

        assertThat(cache.getExecutions(new CacheKey("foo", new int[0]))).isZero();
        assertThat(delegate.getCachedStatementNames()).isEmpty();
    }

    @Test
    void evictDelegates() {

        BoundedStatementCache delegate = new BoundedStatementCache(mock(Client.class), 100);
        ThresholdStatementCache cache = new ThresholdStatementCache(delegate, 2, 100, Long.MAX_VALUE);

        execute(cache, "foo");
        execute(cache, "foo");
        cache.evict("foo");

        assertThat(delegate.getCachedStatementNames()).isEmpty();
        assertThat(execute(cache, "foo")).isEqualTo("S_1");
    }

    @Test
    void rejectsThresholdsThatNeedNoCounting() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ThresholdStatementCache(DisabledStatementCache.INSTANCE, 1, 100, Long.MAX_VALUE));
    }

    /**
     * Mirror {@code ExtendedFlowOperator}: decide on preparation, pick a name, and cache it once the server confirmed the parse.
     */
    private static String execute(StatementCache cache, String sql) {
        Binding binding = new Binding(0);
        boolean prepareRequired = cache.requiresPrepare(binding, sql);
        String name = cache.getName(binding, sql);
        if (prepareRequired || name.isEmpty()) {
            cache.put(binding, sql, name);
        }
        return name;
    }

}
