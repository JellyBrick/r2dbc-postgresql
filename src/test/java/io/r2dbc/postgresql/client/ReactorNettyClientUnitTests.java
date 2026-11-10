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

package io.r2dbc.postgresql.client;

import org.junit.jupiter.api.Test;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpServer;

import java.io.File;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Unit tests for {@link ReactorNettyClient}.
 */
final class ReactorNettyClientUnitTests {

    @Test
    void connectShouldNotLeakFileDescriptors() {

        DisposableServer server = TcpServer.create().host("127.0.0.1").port(0).handle((in, out) -> in.receive().then()).bindNow();

        try {
            connectAndClose(server.port(), 20); // class loading, event loop startup
            int before = openFileDescriptors();

            connectAndClose(server.port(), 100);

            // A DNS resolver per connection retains one UDP socket per connection (gh-756).
            assertThat(openFileDescriptors() - before).isLessThan(50);
        } finally {
            server.disposeNow();
        }
    }

    private static void connectAndClose(int port, int times) {
        for (int i = 0; i < times; i++) {
            ReactorNettyClient.connect("127.0.0.1", port).flatMap(ReactorNettyClient::close).block(Duration.ofSeconds(10));
        }
    }

    private static int openFileDescriptors() {
        // The OperatingSystem MXBean's OpenFileDescriptorCount is unreliable on macOS, so list the descriptors.
        String[] descriptors = new File("/dev/fd").list();
        assumeTrue(descriptors != null, "/dev/fd is not available on this platform");
        return descriptors.length;
    }

}
