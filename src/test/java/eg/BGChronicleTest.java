package eg;

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class BGChronicleTest {

    private static final int CHRONICLE_HEARTBEAT_SECONDS = 5;
    public static final long MAX_NUMBER_OF_EATERIES = 3000000L;
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9050;

    Random rnd = new Random(System.currentTimeMillis());
    byte[] key = new byte[17];
    byte[] value = new byte[11];


    public ChronicleMap<byte[], byte[]> startChronicleMapServer(int port)
            throws InterruptedException, IOException {


        TcpTransportAndNetworkConfig serverConfig = TcpTransportAndNetworkConfig.of(port)
                .heartBeatInterval(CHRONICLE_HEARTBEAT_SECONDS, TimeUnit.SECONDS).name("serverMap")
                .autoReconnectedUponDroppedConnection(true);


        return ChronicleMapBuilder.of(byte[].class, byte[].class)
                .entries(MAX_NUMBER_OF_EATERIES)
                .constantKeySizeBySample(key)
                .constantValueSizeBySample(value)
                .putReturnsNull(true)
                .removeReturnsNull(true)
                .replication((byte) 1, serverConfig)
                .create();
    }

    /**
     * over loop-back on my mac I get around 50,000 messages per second of my local network ( LAN ),
     * I get around 730 messages per second
     *
     * @param hostNameOrIpAddress
     * @param port
     * @param numberOfMessages
     * @throws IOException
     */
    public void startChronicleMapClient(String hostNameOrIpAddress, final int port, final long
            numberOfMessages) throws IOException {

        System.out.println("client starting");


        try (ChronicleMap<byte[], byte[]> map = ChronicleMapBuilder.of(byte[].class, byte[].class)
                .constantKeySizeBySample(key)
                .constantValueSizeBySample(value)
                .putReturnsNull(true)
                .removeReturnsNull(true)
                .statelessClient(new InetSocketAddress(hostNameOrIpAddress, port)).create()) {

            System.out.println("client started");

            long start = System.nanoTime();
            for (long count = 0; count < numberOfMessages; count++) {
                if (rnd.nextBoolean()) {
                    // puts
                    rnd.nextBytes(key);
                    rnd.nextBytes(value);
                    map.put(key, value);

                } else {
                    //gets
                    rnd.nextBytes(key);
                    map.get(key);
                }

                if (count % 50000 == 0) {
                    logMessagesPerSecond(count, start);
                }

            }

            logMessagesPerSecond(numberOfMessages, start);

        }
    }


    /**
     * over loop-back on my mac I get around 50,000 messages per second of my local network ( LAN ),
     * I get around 730 messages per second
     *
     * @param hostNameOrIpAddress
     * @param port
     * @param numberOfMessages
     * @throws IOException
     */
    public void startReplicatingChronicleMapClient(String hostNameOrIpAddress, final int port,
                                                   final long numberOfMessages) throws
            IOException {

        System.out.println("client starting");

        final InetSocketAddress serverHostPort = new InetSocketAddress(hostNameOrIpAddress, port);

        TcpTransportAndNetworkConfig clientConfig =
                TcpTransportAndNetworkConfig.of(port + 1, serverHostPort)
                .heartBeatInterval(CHRONICLE_HEARTBEAT_SECONDS, TimeUnit.SECONDS).name("clientMap")
                .autoReconnectedUponDroppedConnection(true);

        try (ChronicleMap<byte[], byte[]> map = ChronicleMapBuilder.of(byte[].class, byte[].class)
                .entries(MAX_NUMBER_OF_EATERIES)
                .keySize(17)
                .valueSize(11)
                .putReturnsNull(true)
                .removeReturnsNull(true)
                .replication((byte) 2, clientConfig)
                .create()) {

            System.out.println("client started");

            long start = System.nanoTime();
            for (long count = 0; count < numberOfMessages; count++) {
                if (rnd.nextBoolean()) {
                    // puts
                    rnd.nextBytes(key);
                    rnd.nextBytes(value);
                    map.put(key, value);

                } else {
                    //gets
                    rnd.nextBytes(key);
                    map.get(key);
                }

                if (count % 50000 == 0) {
                    logMessagesPerSecond(count, start);
                }

            }

            logMessagesPerSecond(numberOfMessages, start);

        }
    }


    private void logMessagesPerSecond(double numberOfMessages, long start) {
        double messagesPerSecond = numberOfMessages / (double) TimeUnit.NANOSECONDS
                .toSeconds(System.nanoTime() - start);
        System.out.println("messages per seconds " + messagesPerSecond);
    }


    /**
     * for loop back args = []
     *
     * for server - eg.BGChronicleTest server for client - eg.BGChronicleTest client
     *
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String... args) throws IOException, InterruptedException {

        final BGChronicleTest o = new BGChronicleTest();
        ChronicleMap<byte[], byte[]> serverMap = null;
        try {

            // loop-back
            if (args.length == 0) {
                serverMap = o.startChronicleMapServer(DEFAULT_PORT);
                o.startChronicleMapClient(DEFAULT_HOST, DEFAULT_PORT, MAX_NUMBER_OF_EATERIES);

            } else {

                // server
                if (args.length > 0 && args[0].equalsIgnoreCase("server")) {
                    int port = getPort(args, 1);

                    serverMap = o.startChronicleMapServer(port);
                    System.out.println("server ON");
                } else
                    System.out.println("server if OFF");

                // client
                if (args.length == 0 || (args.length > 1 && args[0].equalsIgnoreCase("client"))) {

                    // stateless replicating client
                    String host = getHost(args, 1);
                    int port = getPort(args, 2);
                    System.out.println("client ON....");
                    o.startChronicleMapClient(host, port, MAX_NUMBER_OF_EATERIES);

                    // replicating client
                } else if (args.length > 1 && args[0].equalsIgnoreCase("replicatingClient")) {
                    String host = getHost(args, 1);
                    int port = getPort(args, 2);
                    System.out.println("client ON....");
                    o.startReplicatingChronicleMapClient(host, port, MAX_NUMBER_OF_EATERIES);

                } else {
                    System.out.println("client if OFF");
                    for (; ; ) {
                        Thread.sleep(1000);
                    }
                }

                System.out.println("client if OFF");
                for (; ; ) {
                    Thread.sleep(1000);
                }

            }

        } finally {
            if (serverMap != null)
                serverMap.close();
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

    @Test
    public void test() throws IOException, InterruptedException {
        main();
    }

}
