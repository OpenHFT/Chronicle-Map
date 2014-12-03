package eg;

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class BGChronicleTest {
    private static final int CHRONICLE_HEARTBEAT_SECONDS = 5;
    public static final long MAX_NUMBER_OF_EATERIES = 3000000L;
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9050;

    Random rnd = new Random(System.currentTimeMillis());
    byte[] key = new byte[17];
    byte[] value = new byte[11];


    public ChronicleMap<byte[], byte[]> startChronicleMapServer(int port) throws InterruptedException, IOException {


        TcpTransportAndNetworkConfig map2Config = TcpTransportAndNetworkConfig.of(port)
                .heartBeatInterval(CHRONICLE_HEARTBEAT_SECONDS, TimeUnit.SECONDS).name("serverMap")
                .autoReconnectedUponDroppedConnection(true);


        return ChronicleMapBuilder.of(byte[].class, byte[].class)
                .entries(MAX_NUMBER_OF_EATERIES)
                .keySize(17)
                .valueSize(11)
                .putReturnsNull(true)
                .removeReturnsNull(true)
                .replication((byte) 1, map2Config)
                .create();
    }

    /**
     * over loop-back on my mac I get around 50,000 messages per second
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
                .keySize(17)
                .valueSize(11)
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

    private void logMessagesPerSecond(double numberOfMessages, long start) {
        double messagesPerSecond = numberOfMessages / (double) TimeUnit.NANOSECONDS
                .toSeconds(System.nanoTime() - start);

        System.out.println("messages per seconds " + messagesPerSecond);
    }


    @Test(timeout = 10000)
    public void testBufferOverFlowPutAllAndEntrySet() throws IOException, InterruptedException {
        int SIZE = 10;
        int port = 9050;
        try (ChronicleMap<Integer, CharSequence> serverMap = ChronicleMapBuilder
                .of(Integer.class, CharSequence.class)
                .entries(SIZE)
                .replication((byte) 2, TcpTransportAndNetworkConfig.of(port))
                .create()) {
            try (ChronicleMap<Integer, CharSequence> statelessMap = ChronicleMapBuilder
                    .of(Integer.class, CharSequence.class)
                    .statelessClient(new InetSocketAddress("localhost", port))
                    .create()) {
                Map<Integer, CharSequence> payload = new HashMap<Integer, CharSequence>();

                for (int i = 0; i < SIZE; i++) {
                    payload.put(i, "some value=" + i);
                }

                statelessMap.putAll(payload);

                Set<Map.Entry<Integer, CharSequence>> entries = statelessMap.entrySet();

                Map.Entry<Integer, CharSequence> next = entries.iterator().next();
                assertEquals("some value=" + next.getKey(), next.getValue());

                assertEquals(SIZE, entries.size());
            }
        }
    }


    /**
     * for loop back args = []
     *
     * for servereg.BGChronicleTest server
     *
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String... args) throws IOException, InterruptedException {

        final BGChronicleTest o = new BGChronicleTest();
        ChronicleMap<byte[], byte[]> serverMap = null;
        try {

            if (args.length == 0 || (args.length > 0 && args[0].equalsIgnoreCase("server"))) {
                int port = args.length > 1 && args[1] != null ? Integer.valueOf(args[1]) :
                        DEFAULT_PORT;

                serverMap = o.startChronicleMapServer(port);
                System.out.println("server ON");
            }   else
                System.out.println("server if OFF");

            if (args.length == 0 || (args.length > 1 && args[0].equalsIgnoreCase("client"))) {
                String host = args.length > 1 && args[1] != null ? args[1] : DEFAULT_HOST;
                int port = args.length > 2 && args[2] != null ? Integer.valueOf(args[2]) :
                        DEFAULT_PORT;
                System.out.println("client ON....");
                o.startChronicleMapClient(host, port, MAX_NUMBER_OF_EATERIES);
            } else {
                System.out.println("client if OFF");
            }

        } finally {
            if (serverMap != null)
                serverMap.close();
        }

    }

    public void test() throws IOException, InterruptedException {
        main();
    }

}
