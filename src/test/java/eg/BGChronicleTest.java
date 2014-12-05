package eg;

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static java.lang.System.nanoTime;
import static java.nio.ByteBuffer.wrap;

public class BGChronicleTest {

    private static final int CHRONICLE_HEARTBEAT_SECONDS = 5;
    public static final int CLIENTS = Integer.getInteger("clients", 10);
    public static final long KEYS = Long.getLong("keys", 100000L);
    public static final long MESSAGES = Long.getLong("messages", 200000L);
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9050;
    public static final int KEY_SIZE = Integer.getInteger("keySize", 16);
    public static final int VALUE_SIZE = Integer.getInteger("valueSize", 16);
    private volatile long start;


    public ChronicleMap<byte[], byte[]> startChronicleMapServer(int port) throws InterruptedException, IOException {


        TcpTransportAndNetworkConfig serverConfig = TcpTransportAndNetworkConfig.of(port)
                .heartBeatInterval(CHRONICLE_HEARTBEAT_SECONDS, TimeUnit.SECONDS)
                .name("serverMap")
                .autoReconnectedUponDroppedConnection(true);

        File file = File.createTempFile("bg-test", ".deleteme");
        file.deleteOnExit();
        return ChronicleMapBuilder.of(byte[].class, byte[].class)
                .entries(KEYS)
                .constantKeySizeBySample(new byte[KEY_SIZE])
                .constantValueSizeBySample(new byte[VALUE_SIZE])
                .putReturnsNull(true)
                .removeReturnsNull(true)
                .replication((byte) 1, serverConfig)
                .createPersistedTo(file);
    }

    /**
     * over loop-back on my mac I get around 50,000 messages per second of my local network ( LAN )
     */
    public void startChronicleMapClient(String hostNameOrIpAddress, int port) throws IOException, InterruptedException {

        System.out.println("client starting");
        final InetSocketAddress remoteAddress = new InetSocketAddress(hostNameOrIpAddress, port);

        start = nanoTime();

        ExecutorService es = Executors.newFixedThreadPool(CLIENTS);
        List<Future<Void>> futureList = new ArrayList<>();
        for (int i = 0; i < CLIENTS; i++) {
            final int clientId = i;
            futureList.add(es.submit(new Callable<Void>() {
                @Override
                public Void call() throws IOException {
                    try (ChronicleMap<byte[], byte[]> map = ChronicleMapBuilder
                            .of(byte[].class, byte[].class)
                            .constantKeySizeBySample(new byte[KEY_SIZE])
                            .constantValueSizeBySample(new byte[VALUE_SIZE])
                            .putReturnsNull(true)
                            .removeReturnsNull(true)
                            .statelessClient(remoteAddress)
                            .create()) {

                        System.out.println(clientId + "/" + CLIENTS + " client started");

                        exerciseMap(MESSAGES, map, clientId);
                        System.out.println(clientId + "/" + CLIENTS + " ... client finished");
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
        es.shutdown();
        logMessagesPerSecond(MESSAGES, start);
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
                if (rnd.nextBoolean()) {
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


    /**
     * over loop-back on my mac I get around 50,000 messages per second of my local network ( LAN ),
     */
    public void startReplicatingChronicleMapClient(String hostNameOrIpAddress, final int port)
            throws IOException, InterruptedException {

        System.out.println("clients starting");

        final InetSocketAddress serverHostPort = new InetSocketAddress(hostNameOrIpAddress, port);

        final TcpTransportAndNetworkConfig clientConfig = TcpTransportAndNetworkConfig.of(port + 1, serverHostPort)
                .heartBeatInterval(CHRONICLE_HEARTBEAT_SECONDS, TimeUnit.SECONDS).name("clientMap")
                .autoReconnectedUponDroppedConnection(true);

        start = nanoTime();
        ExecutorService es = Executors.newFixedThreadPool(CLIENTS);
        for (int i = 0; i < CLIENTS; i++) {
            final int clientId = i;
            es.submit(new Runnable() {
                @Override
                public void run() {
                    try (ChronicleMap<byte[], byte[]> map = ChronicleMapBuilder.of(byte[].class, byte[].class)
                            .entries(KEYS)
                            .constantKeySizeBySample(new byte[KEY_SIZE])
                            .constantValueSizeBySample(new byte[VALUE_SIZE])
                            .putReturnsNull(true)
                            .removeReturnsNull(true)
                            .replication((byte) (2 + clientId), clientConfig)
                            .create()) {

                        System.out.println(clientId + "/" + CLIENTS + " client started");

                        exerciseMap(MESSAGES, map, clientId);
                        System.out.println(clientId + "/" + CLIENTS + " ... client finished");
                    }

                }
            });
        }
        es.awaitTermination(1, TimeUnit.HOURS);
        logMessagesPerSecond(MESSAGES, start);
    }


    private void logMessagesPerSecond(double numberOfMessages, long start) {
        long messagesPerSecond = (long) (numberOfMessages * 1e9 / (System.nanoTime() - start));
        System.out.printf("messages per seconds: %,d%n", messagesPerSecond);
    }


    /**
     * for loop back args = []
     * <p/>
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
            try (ChronicleMap<byte[], byte[]> serverMap = o.startChronicleMapServer(DEFAULT_PORT)) {
                o.startChronicleMapClient(DEFAULT_HOST, DEFAULT_PORT);
            }

        } else switch (args[0].toLowerCase()) {
            case "server": {
                int port = getPort(args, 1);

                o.startChronicleMapServer(port);
                System.out.println("server ON");

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

    @Ignore
    @Test
    public void test() throws IOException, InterruptedException {
        main();
    }

}
