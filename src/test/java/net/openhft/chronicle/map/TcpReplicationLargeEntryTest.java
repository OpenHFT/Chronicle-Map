package net.openhft.chronicle.map;

import junit.framework.Assert;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author Rob Austin.
 */
public class TcpReplicationLargeEntryTest {

    private ChronicleMap<Integer, CharSequence> map1;
    private ChronicleMap<Integer, CharSequence> map2;

    static int s_port = 8010;
    int valueSize = 1_00_000; // 1MB

    char[] valueX = new char[valueSize - 100];
    char[] valueY = new char[valueSize - 100];

    @Before
    public void setup() throws IOException {
        Arrays.fill(valueX, 'X');
        Arrays.fill(valueX, 'Y');

        int keySize = 4;
        int entrySize = keySize + valueSize;

        final InetSocketAddress endpoint = new InetSocketAddress("localhost", s_port + 1);

        {
            final TcpTransportAndNetworkConfig tcpConfig1 = TcpTransportAndNetworkConfig.of(s_port,
                    endpoint).autoReconnectedUponDroppedConnection(true).name("      map1")
                    .heartBeatInterval(1, TimeUnit.SECONDS)
                    .tcpBufferSize(1024 * 64);


            map1 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .entrySize(entrySize)
                    .entries(2)
                    .actualSegments(1)
                    .replication((byte) 1, tcpConfig1)
                    .instance()
                    .name("map1")
                    .create();
        }
        {
            final TcpTransportAndNetworkConfig tcpConfig2 = TcpTransportAndNetworkConfig.of
                    (s_port + 1).autoReconnectedUponDroppedConnection(true).name("map2")
                    .heartBeatInterval(1, TimeUnit.SECONDS)
                    .tcpBufferSize(1024 * 64);

            map2 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .entrySize(entrySize)
                    .entries(2)
                    .replication((byte) 2, tcpConfig2)
                    .instance()
                    .name("map2")
                    .create();

        }
        s_port += 2;
    }


    @After
    public void tearDown() throws InterruptedException {

        for (final Closeable closeable : new Closeable[]{map1, map2}) {
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.gc();
    }

    Set<Thread> threads;

    @Before
    public void sampleThreads() {
        threads = Thread.getAllStackTraces().keySet();
    }

    @After
    public void checkThreadsShutdown() {
        StatelessClientTest.checkThreadsShutdown(threads);
    }


    @Test
    public void testLargeValues() throws IOException, InterruptedException {

        String xString = new String(valueX);
        String yString = new String(valueY);

        map1.put(1, xString);
        map1.put(2, yString);

        System.out.println("\nwaiting till equal");

        waitTillEqual(10000);

        Assert.assertEquals(xString, map2.get(1));
        Assert.assertEquals(yString, map2.get(2));

    }


    private void waitTillEqual(final int timeOutMs) throws InterruptedException {

        Map map1UnChanged = new HashMap();
        Map map2UnChanged = new HashMap();

        int numberOfTimesTheSame = 0;
        for (int t = 0; t < timeOutMs + 100; t++) {
            if (map1.equals(map2)) {
                if (map1.equals(map1UnChanged) && map2.equals(map2UnChanged)) {
                    numberOfTimesTheSame++;
                } else {
                    numberOfTimesTheSame = 0;
                    map1UnChanged = new HashMap(map1);
                    map2UnChanged = new HashMap(map2);
                }
                Thread.sleep(1);
                if (numberOfTimesTheSame == 100) {
                    System.out.println("same");
                    break;
                }

            }
            Thread.sleep(1);
        }
    }
}

