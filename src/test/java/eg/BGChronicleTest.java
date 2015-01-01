package eg;

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.ChronicleMapStatelessClientBuilder;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static java.lang.System.nanoTime;
import static java.nio.ByteBuffer.wrap;

public class BGChronicleTest {

    public static final int CHRONICLE_HEARTBEAT_SECONDS = 5;
    public static final int READ_RATIO = Integer.getInteger("readRatio", 2);
    //    public static final boolean BUSY_WAIT = Boolean.parseBoolean(System.getProperty("busyWait",
//            "" + (CLIENTS < Runtime.getRuntime().availableProcessors())));
    public static final int REPLICAS = Integer.getInteger("replicas", 1);
    public static final int CLIENTS = Integer.getInteger("clients", Math.max(1, Runtime.getRuntime().availableProcessors() - REPLICAS));
    public static final long KEYS = Long.getLong("keys", 1000_000L);
    public static final long MESSAGES = Long.getLong("messages", 1000_000 + KEYS * 2);
    public static final long MAX_RATE = Long.getLong("maxRate", 100000);
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9050;
    public static final int KEY_SIZE = Integer.getInteger("keySize", 16);
    public static final int VALUE_SIZE = Integer.getInteger("valueSize", 16);
    private volatile long start;

    public Closeable startChronicleMapServer(int port) throws InterruptedException, IOException {
        TcpTransportAndNetworkConfig serverConfig = TcpTransportAndNetworkConfig.of(port)
                .heartBeatInterval(CHRONICLE_HEARTBEAT_SECONDS, TimeUnit.SECONDS)
                .name("serverMap")
                .autoReconnectedUponDroppedConnection(true);

        File file = File.createTempFile("bg-test", ".deleteme");
        file.deleteOnExit();
        final ChronicleMap<byte[], byte[]> replica1 = ChronicleMapBuilder.of(byte[].class, byte[].class)
                .entries(KEYS)
                .constantKeySizeBySample(new byte[KEY_SIZE])
                .constantValueSizeBySample(new byte[VALUE_SIZE])
                .putReturnsNull(true)
                .removeReturnsNull(true)
                .replication((byte) 1, serverConfig)
                .createPersistedTo(file);

        if (REPLICAS == 1)
            return replica1;

        File file2 = File.createTempFile("bg-test", ".deleteme");
        file2.deleteOnExit();
        final InetSocketAddress serverHostPort = new InetSocketAddress("localhost", port);

        final TcpTransportAndNetworkConfig clientConfig = TcpTransportAndNetworkConfig.of(port + 1, serverHostPort)
                .heartBeatInterval(CHRONICLE_HEARTBEAT_SECONDS, TimeUnit.SECONDS).name("clientMap")
                .autoReconnectedUponDroppedConnection(true);

        final ChronicleMap<byte[], byte[]> replica2 = ChronicleMapBuilder.of(byte[].class, byte[].class)
                .entries(KEYS)
                .constantKeySizeBySample(new byte[KEY_SIZE])
                .constantValueSizeBySample(new byte[VALUE_SIZE])
                .putReturnsNull(true)
                .removeReturnsNull(true)
                .replication((byte) 2, clientConfig)
                .createPersistedTo(file2);

        return new Closeable() {
            @Override
            public void close() throws IOException {
                replica1.close();
                replica2.close();
            }
        };
    }

    /**
     * over loop-back on my mac I get around 50,000 messages per second of my local network ( LAN )
     */
    public void startChronicleMapClient(String hostNameOrIpAddress, int port) throws IOException, InterruptedException {

        final InetSocketAddress remoteAddress = new InetSocketAddress(hostNameOrIpAddress, port);
        final InetSocketAddress remoteAddress2 = new InetSocketAddress(hostNameOrIpAddress, port + 1);
        System.out.println("client connecting to " + remoteAddress);

        System.out.println("###  Throughput test  ###");
        start = nanoTime();
        ExecutorService es = Executors.newFixedThreadPool(CLIENTS);
        List<Future<Void>> futureList = new ArrayList<>();
        for (int i = 0; i < CLIENTS; i++) {
            final int clientId = i;
            futureList.add(es.submit(new Callable<Void>() {
                @Override
                public Void call() throws IOException {
                    InetSocketAddress serverAddress =
                            REPLICAS > 1 && clientId % 2 == 0 ? remoteAddress2 : remoteAddress;
                    try (ChronicleMap<byte[], byte[]> map = ChronicleMapStatelessClientBuilder
                            .<byte[], byte[]>of(serverAddress)
                            .putReturnsNull(true)
                            .removeReturnsNull(true)
                            .create()) {

                        System.out.println((clientId + 1) + "/" + CLIENTS + " client started");

                        exerciseMap(MESSAGES, map, clientId);
                        System.out.println((clientId + 1) + "/" + CLIENTS + " ... client finished");
                    }
                    return null;
                }
            }));
        }
        for (Future<Void> future : futureList) {
            try {
                future.get();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        futureList.clear();
        long throughput = logMessagesPerSecond(MESSAGES, start);
        // 50% of throughput, rounded to multiple of 500.
        long rate = Math.min((throughput / 2) / 500 * 500, MAX_RATE);
        System.out.printf("### Latency test with a target throughput of %,d msg/sec%n", rate);

        final long delayNS = CLIENTS * 1_000_000_000L / rate;
        List<Future<long[]>> futureList2 = new ArrayList<>();
        long start2 = System.nanoTime();
        for (int i = 0; i < CLIENTS; i++) {
            final int clientId = i;
            futureList2.add(es.submit(new Callable<long[]>() {
                @Override
                public long[] call() throws IOException, InterruptedException {
                    try (ChronicleMap<byte[], byte[]> map = ChronicleMapStatelessClientBuilder
                            .<byte[], byte[]>of(remoteAddress)
                            .putReturnsNull(true)
                            .removeReturnsNull(true)
                            .create()) {
                        map.size();

                        System.out.println((clientId + 1) + "/" + CLIENTS + " client started");

                        long[] times = exerciseMapLatency(MESSAGES, map, delayNS - clientId * 1000);
                        System.out.println((clientId + 1) + "/" + CLIENTS + " ... client finished");
                        return times;
                    }
                }
            }));
        }
        long[] times = new long[(int) MESSAGES];
        int offset = 0;
        for (Future<long[]> future : futureList2) {
            try {
                long[] t2 = future.get();
                System.arraycopy(t2, 0, times, offset, t2.length);
                offset += t2.length;
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        long time2 = System.nanoTime() - start2;
        long rate2 = (long) (MESSAGES * 1e9 / time2);
        System.out.printf("Rate achieved was %,d msg/sec%n", rate2);
        Arrays.sort(times);
        System.out.printf("50%% / 90%% / 99%% // 99.9%% / 99.99%% / worst latency was %,d / %,d / %,d // %,d / %,d / %,d us%n",
                times[times.length - times.length / 2] / 1000,
                times[times.length - times.length / 10] / 1000,
                times[times.length - times.length / 100] / 1000,
                times[times.length - times.length / 1000] / 1000,
                times[times.length - times.length / 10000] / 1000,
                times[times.length - 1] / 1000
        );

        es.shutdown();
    }

    public void exerciseMap(long numberOfMessages, ChronicleMap<byte[], byte[]> map, int clientId) {
        byte[] key = new byte[KEY_SIZE];
        byte[] value = new byte[VALUE_SIZE];
        Random rnd = new Random();
        ByteBuffer keyBB = wrap(key);
        for (long count = 0; count < numberOfMessages; count += CLIENTS * 100) {
            // KEYS/100 possible values after first byte.
            keyBB.putInt(1, rnd.nextInt((int) (KEYS / 100)));
            rnd.nextBytes(value);
            for (byte b = 0; b < 100; b++) {
                // 100 possible values for the first byte.
                key[0] = b;
                if (rnd.nextInt(READ_RATIO + 1) == 0) {
                    // puts
                    value[0] = b;
                    map.put(key, value);

                } else {
                    //gets
                    map.get(key);
                }
            }

            if (clientId == 0 && count > 0 && count % 50000 == 0) {
                logMessagesPerSecond(count, start);
            }

        }
    }

    public long[] exerciseMapLatency(long numberOfMessages, ChronicleMap<byte[], byte[]> map, long delayNS) throws InterruptedException {
        long[] times = new long[(int) (numberOfMessages / CLIENTS)];
        byte[] key = new byte[KEY_SIZE];
        byte[] value = new byte[VALUE_SIZE];
        Random rnd = new Random();
        ByteBuffer keyBB = wrap(key);
        long next = System.nanoTime() + delayNS;
        for (int count = 0; count < times.length; count += 100) {
            // KEYS/100 possible values after first byte.
            keyBB.putInt(1, rnd.nextInt((int) (KEYS / 100)));
            rnd.nextBytes(value);
            for (byte b = 0; b < 100; b++) {
                next += delayNS;
                long now = System.nanoTime();
                int pause = (int) ((next - now) / 1000000);
                if (pause > 0)
                    Thread.sleep(pause);
                // wait for residual.
//                if (BUSY_WAIT)
//                    while (System.nanoTime() < next) ;
                // 100 possible values for the first byte.
                long start;
                key[0] = b;
                if (rnd.nextInt(READ_RATIO + 1) == 0) {
                    // puts
                    value[0] = b;
                    start = System.nanoTime();
                    map.put(key, value);

                } else {
                    //gets
                    start = System.nanoTime();
                    map.get(key);
                }
                long time = System.nanoTime() - start;
                if (count + b < times.length)
                    times[count + b] = time;
            }
        }
        return times;
    }


    /**
     * over loop-back on my mac I get around 50,000 messages per second of my local network ( LAN ),
     */
    public void startReplicatingChronicleMapClient(String hostNameOrIpAddress, final int port)
            throws IOException, InterruptedException {

        final InetSocketAddress serverHostPort = new InetSocketAddress(hostNameOrIpAddress, port);

        final TcpTransportAndNetworkConfig clientConfig = TcpTransportAndNetworkConfig.of(port + 1, serverHostPort)
                .heartBeatInterval(CHRONICLE_HEARTBEAT_SECONDS, TimeUnit.SECONDS).name("clientMap")
                .autoReconnectedUponDroppedConnection(true);

        System.out.println("clients connecting to " + serverHostPort);

        System.out.println("###  Throughput test  ###");
        start = nanoTime();
        ExecutorService es = Executors.newFixedThreadPool(CLIENTS);
        List<Future<Void>> futureList = new ArrayList<>();
        for (int i = 0; i < CLIENTS; i++) {
            final int clientId = i;
            futureList.add(es.submit(new Callable<Void>() {
                @Override
                public Void call() throws IOException {
                    try (ChronicleMap<byte[], byte[]> map = ChronicleMapBuilder.of(byte[].class, byte[].class)
                            .entries(KEYS)
                            .constantKeySizeBySample(new byte[KEY_SIZE])
                            .constantValueSizeBySample(new byte[VALUE_SIZE])
                            .putReturnsNull(true)
                            .removeReturnsNull(true)
                            .replication((byte) (10 + clientId), clientConfig)
                            .create()) {

                        System.out.println((clientId + 1) + "/" + CLIENTS + " client started");

                        exerciseMap(MESSAGES, map, clientId);
                        System.out.println((clientId + 1) + "/" + CLIENTS + " ... client finished");
                    }
                    return null;
                }
            }));
        }
        for (Future<Void> future : futureList) {
            try {
                future.get();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        futureList.clear();
        long throughput = logMessagesPerSecond(MESSAGES, start);
        // 50% of throughput, rounded to multiple of 100.
        long rate = Math.min((throughput / 2) / 100 * 100, MAX_RATE);
        System.out.printf("### Latency test with a target throughput of %,d msg/sec%n", rate);

        final long delayNS = CLIENTS * 1_000_000_000L / rate;
        List<Future<long[]>> futureList2 = new ArrayList<>();
        long start2 = System.nanoTime();
        for (int i = 0; i < CLIENTS; i++) {
            final int clientId = i;
            futureList2.add(es.submit(new Callable<long[]>() {
                @Override
                public long[] call() throws IOException, InterruptedException {
                    try (ChronicleMap<byte[], byte[]> map = ChronicleMapBuilder.of(byte[].class, byte[].class)
                            .entries(KEYS)
                            .constantKeySizeBySample(new byte[KEY_SIZE])
                            .constantValueSizeBySample(new byte[VALUE_SIZE])
                            .putReturnsNull(true)
                            .removeReturnsNull(true)
                            .replication((byte) (10 + clientId), clientConfig)
                            .create()) {
                        map.size();

                        System.out.println((clientId + 1) + "/" + CLIENTS + " client started");

                        long[] times = exerciseMapLatency(MESSAGES, map, delayNS - clientId * 1000);
                        System.out.println((clientId + 1) + "/" + CLIENTS + " ... client finished");
                        return times;
                    }
                }
            }));
        }
        long[] times = new long[(int) MESSAGES];
        int offset = 0;
        for (Future<long[]> future : futureList2) {
            try {
                long[] t2 = future.get();
                System.arraycopy(t2, 0, times, offset, t2.length);
                offset += t2.length;
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        long time2 = System.nanoTime() - start2;
        long rate2 = (long) (MESSAGES * 1e9 / time2);
        System.out.printf("Rate achieved was %,d msg/sec%n", rate2);
        Arrays.sort(times);
        System.out.printf("50%% / 90%% / 99%% // 99.9%% / 99.99%% / worst latency was %,d / %,d / %,d // %,d / %,d / %,d us%n",
                times[times.length - times.length / 2] / 1000,
                times[times.length - times.length / 10] / 1000,
                times[times.length - times.length / 100] / 1000,
                times[times.length - times.length / 1000] / 1000,
                times[times.length - times.length / 10000] / 1000,
                times[times.length - 1] / 1000
        );

        es.shutdown();
    }


    private long logMessagesPerSecond(double numberOfMessages, long start) {
        long messagesPerSecond = (long) (numberOfMessages * 1e9 / (System.nanoTime() - start));
        System.out.printf("messages per seconds: %,d%n", messagesPerSecond);
        return messagesPerSecond;
    }


    /**
     * for loop back args = []
     * <p>
     * for server - eg.BGChronicleTest server for client - eg.BGChronicleTest client
     *
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String... args) throws IOException, InterruptedException {

        final BGChronicleTest o = new BGChronicleTest();

        // loop-back
        if (args.length == 0) {
            try (Closeable serverMap = o.startChronicleMapServer(DEFAULT_PORT)) {
                o.startChronicleMapClient(DEFAULT_HOST, DEFAULT_PORT);
            }

        } else switch (args[0].toLowerCase()) {
            case "server": {
                int port = getPort(args, 1);

                o.startChronicleMapServer(port);
                System.out.println("server ON");
                System.in.read();
                System.out.println("... server OFF");
                break;
            }

            case "client": {
                // stateless replicating client
                String host = getHost(args, 1);
                int port = getPort(args, 2);
                System.out.println("client ON....");
                o.startChronicleMapClient(host, port);
                break;
            }

            case "replicatingClient": {
                String host = getHost(args, 1);
                int port = getPort(args, 2);
                System.out.println("client ON....");
                o.startReplicatingChronicleMapClient(host, port);
                break;
            }
            default:
                System.err.println("Usage: java " + BGChronicleTest.class.getName() + " server {port}");
                System.err.println("\tjava " + BGChronicleTest.class.getName() + " client {hostname} {port}");
                System.err.println("\tjava " + BGChronicleTest.class.getName() + " replicatingClient {hostname} {port}");
                break;
        }
    }

    private static String getHost(String[] args, int index) {
        String host = args.length > index && args[index] != null ? args[index] : DEFAULT_HOST;
        System.out.println(host);
        return host;
    }

    private static int getPort(String[] args, int index) {
        int port = args.length > index && args[index] != null ? Integer.valueOf(args[index]) :
                DEFAULT_PORT;
        System.out.println(port);
        return port;
    }


    public void test() throws IOException, InterruptedException {
        main();
    }

}
