/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static net.openhft.chronicle.map.ChronicleMapStatelessClientBuilder.createClientOf;

public class StatelessClientDuplicateKeyTest {

    @Test
    public void statelessClientDuplicateIntegerKeyTest()
            throws IOException, InterruptedException, ExecutionException {
        ChronicleMap<Integer, Integer> map =
                ChronicleMapBuilder.of(Integer.class, Integer.class)
                .replication((byte) 1, TcpTransportAndNetworkConfig.of(8080))
                .create();

        map.put(42, 1);

        final ChronicleMap<Integer, Integer> client =
                createClientOf(new InetSocketAddress("localhost", 8080));

        ExecutorService runner = new ThreadPoolExecutor(16, 256, 300L, TimeUnit.SECONDS,
                new LinkedBlockingDeque<Runnable>());

        class Run implements Callable<Integer> {

            @Override
            public Integer call() {
                return client.get(42);
            }
        }
        Collection<Run> items = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            items.add(new Run());
        }
        List<Future<Integer>> futures = runner.invokeAll(items);
        for (Future<Integer> future : futures) {
            Assert.assertEquals((Integer) 1, future.get());
        }
    }

    @Test
    public void statelessClientDuplicateBytesKeyTest()
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        ChronicleMap<byte[], byte[]> map =
                ChronicleMapBuilder.of(byte[].class, byte[].class)
                        .averageKeySize(10).averageValueSize(10)
                        .entries(100)
                        .actualSegments(4)
                        .replication((byte) 1, TcpTransportAndNetworkConfig.of(8081))
                        .create();

        byte[] _42 = {42};
        map.put(_42, new byte[] {1, 2, 3});
        map.put(new byte[]{1, 2, 3}, _42);
        byte[] longKey = "fooBarFooBarFooBarFooBar".getBytes(ISO_8859_1);
        map.put(longKey, "bar".getBytes(ISO_8859_1));

        final ChronicleMap<byte[], byte[]> client =
                createClientOf(new InetSocketAddress("localhost", 8081));

        ExecutorService runner = new ThreadPoolExecutor(16, 256, 300L, TimeUnit.SECONDS,
                new LinkedBlockingDeque<Runnable>());

        class Run implements Callable<byte[]> {
            final byte[] key;

            Run(byte[] key) {
                this.key = key;
            }

            @Override
            public byte[] call() {
                return client.get(key);
            }
        }

        List<Run> items = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            byte[] bs = i % 3 == 0 ? _42 : (i % 3 == 1 ? new byte[] {(byte) i} : longKey);
            items.add(new Run(bs));
        }

        List<Future<byte[]>> futures = runner.invokeAll(items);
        for (Future<byte[]> future : futures) {
            future.get(1, TimeUnit.MILLISECONDS);
        }
    }
}
