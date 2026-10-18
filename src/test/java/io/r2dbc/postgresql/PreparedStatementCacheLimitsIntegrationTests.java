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

import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.PostgresqlStatement;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@code preparedStatementCacheSizeMiB} and {@code prepareThreshold}.
 */
class PreparedStatementCacheLimitsIntegrationTests extends AbstractIntegrationTests {

    @Test
    void queryIsPreparedOnTheThresholdExecution() {
        withConnection(builder -> builder.prepareThreshold(3), connection -> {

            assertThat(countIn(connection, 1)).isEqualTo(1);
            assertThat(countIn(connection, 1)).isEqualTo(1);
            assertThat(preparedStatements(connection)).isZero();

            assertThat(countIn(connection, 1)).isEqualTo(1);
            assertThat(preparedStatements(connection)).isOne();

            assertThat(countIn(connection, 1)).isEqualTo(1);
            assertThat(preparedStatements(connection)).isOne();
        });
    }

    @Test
    void queriesBelowTheThresholdLeaveNoServerSideStatements() {
        withConnection(builder -> builder.prepareThreshold(5), connection -> {

            for (int size = 2; size <= 61; size++) {
                assertThat(countIn(connection, size)).isEqualTo(Math.min(size, 10));
            }

            assertThat(preparedStatements(connection)).isZero();
        });
    }

    @Test
    void queryAboveHalfOfTheSizeLimitIsNeverPrepared() {
        withConnection(builder -> builder.preparedStatementCacheSizeMiB(1), connection -> {

            // 20,000 placeholders make the SQL text exceed the 512 KiB estimate allowed per query.
            for (int i = 0; i < 3; i++) {
                assertThat(countIn(connection, 20_000)).isEqualTo(10);
            }
            assertThat(preparedStatements(connection)).isZero();

            assertThat(countIn(connection, 1)).isEqualTo(1);
            assertThat(preparedStatements(connection)).isOne();
        });
    }

    private void withConnection(Consumer<PostgresqlConnectionConfiguration.Builder> customizer, Consumer<PostgresqlConnection> test) {
        PostgresqlConnection connection = getConnectionFactory(customizer).create().block();
        try {
            test.accept(connection);
        } finally {
            connection.close().as(StepVerifier::create).verifyComplete();
        }
    }

    private static long countIn(PostgresqlConnection connection, int size) {
        String placeholders = IntStream.rangeClosed(1, size).mapToObj(i -> "$" + i).collect(Collectors.joining(", "));
        PostgresqlStatement statement = connection.createStatement("SELECT count(*) FROM generate_series(1, 10) g WHERE g IN (" + placeholders + ")");
        for (int i = 0; i < size; i++) {
            statement.bind(i, i + 1);
        }
        return Flux.from(statement.execute()).flatMap(result -> result.map((row, metadata) -> row.get(0, Long.class))).single().block();
    }

    private static long preparedStatements(PostgresqlConnection connection) {
        return Flux.from(connection.createStatement("SELECT count(*) FROM pg_prepared_statements").execute())
            .flatMap(result -> result.map((row, metadata) -> row.get(0, Long.class)))
            .single()
            .block();
    }

}
