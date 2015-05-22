package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.replication.SingleChronicleHashReplication;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.IntValue;
import org.junit.*;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author Rob Austin.
 */
@Ignore
public class TcpReplicationSoakTest {

    private ChronicleMap<Integer, CharSequence> map1;
    private ChronicleMap<Integer, CharSequence> map2;
    private IntValue value;
    static int s_port = 8093;

    @Before
    public void setup() throws IOException {
        value = DataValueClasses.newDirectReference(IntValue.class);
        ((Byteable) value).bytes(new ByteBufferBytes(ByteBuffer.allocateDirect(4)), 0);

        final InetSocketAddress endpoint = TcpUtil.localPort(s_port + 1);

        {
            final TcpTransportAndNetworkConfig tcpConfig1 = TcpTransportAndNetworkConfig.of(s_port,
                    endpoint).autoReconnectedUponDroppedConnection(true)
                    .heartBeatInterval(1, TimeUnit.SECONDS)
                    .tcpBufferSize(1024 * 64);

            map1 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .entries(Builder.SIZE + Builder.SIZE)
                    .actualSegments(1)
                    .replication(SingleChronicleHashReplication.builder()
                            .tcpTransportAndNetwork(tcpConfig1)
                            .name("map1")
                            .createWithId((byte) 1))
                    .instance()
                    .name("map1")
                    .create();
        }
        {
            final TcpTransportAndNetworkConfig tcpConfig2 = TcpTransportAndNetworkConfig.of
                    (s_port + 1).autoReconnectedUponDroppedConnection(true)
                    .heartBeatInterval(1, TimeUnit.SECONDS)
                    .tcpBufferSize(1024 * 64);

            map2 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .entries(Builder.SIZE + Builder.SIZE)
                    .replication(SingleChronicleHashReplication.builder()
                            .tcpTransportAndNetwork(tcpConfig2)
                            .name("map2")
                            .createWithId((byte) 2))
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
    public void testSoakTestWithRandomData() throws IOException, InterruptedException {
        try {
            System.out.print("SoakTesting ");
            for (int j = 1; j < 2 * Builder.SIZE; j++) {
                if (j % 1000 == 0)
                    System.out.print(".");
                Random rnd = new Random(j);
                for (int i = 1; i < 10; i++) {
                    final int select = rnd.nextInt(2);
                    final ChronicleMap<Integer, CharSequence> map = select > 0 ? map1 : map2;

                    if (rnd.nextBoolean()) {
                        map.put(rnd.nextInt(Builder.SIZE), "test" + j);

                    } else {
                        map.remove(rnd.nextInt(Builder.SIZE));
                    }
                }
            }

            System.out.println("\nwaiting till equal");

            waitTillEqual(15000);

            Assert.assertEquals(map1, map2);
        } finally {
            map1.close();
            map2.close();
        }

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

