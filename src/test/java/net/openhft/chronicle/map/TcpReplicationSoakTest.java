package net.openhft.chronicle.map;

import junit.framework.Assert;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import net.openhft.lang.values.IntValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author Rob Austin.
 */
public class TcpReplicationSoakTest {


    private ChronicleMap<Integer, CharSequence> map1;
    private ChronicleMap<Integer, CharSequence> map2;
    private IntValue value;
    static int s_port = 8010;

    @Before
    public void setup() throws IOException {
        value = DataValueClasses.newDirectReference(IntValue.class);
        ((Byteable) value).bytes(new ByteBufferBytes(ByteBuffer.allocateDirect(4)), 0);

        final InetSocketAddress endpoint = new InetSocketAddress("localhost", s_port + 1);

        {
            final TcpTransportAndNetworkConfig tcpConfig1 = TcpTransportAndNetworkConfig.of(s_port,
                    endpoint)
                    .heartBeatInterval(1L, TimeUnit.SECONDS)
                    .autoReconnectedUponDroppedConnection(true);


            map1 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .entries(Builder.SIZE)
                    .replication((byte) 1, tcpConfig1)
                    .create();
        }
        {
            final TcpTransportAndNetworkConfig tcpConfig2 = TcpTransportAndNetworkConfig.of(s_port + 1)
                    .heartBeatInterval(1L, TimeUnit.SECONDS)
                    .autoReconnectedUponDroppedConnection(true);

            map2 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .entries(Builder.SIZE)
                    .replication((byte) 2, tcpConfig2)
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


    // TODO test this with larger sizes.
    @Test(timeout = 20000)
    @Ignore("Doesn't work, maps need to check equality.")
    public void testSoakTestWithRandomData() throws IOException, InterruptedException {

        Thread.sleep(100);
        final long start = System.currentTimeMillis();
        System.out.print("SoakTesting ");
        for (int j = 1; j < 2 * Builder.SIZE; j++) {
            if (j % 100 == 0)
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

        waitTillEqual(1000);

        Assert.assertEquals(map1, map2);

    }

    private void waitTillEqual(final int timeOutMs) throws InterruptedException {
        int numberOfTimesTheSame = 0;
        for (int t = 0; t < timeOutMs + 100; t++) {
            if (map1.equals(map2)) {
                numberOfTimesTheSame++;
                Thread.sleep(1);
                if (numberOfTimesTheSame == 100)
                    break;
            } else numberOfTimesTheSame = 0;
            Thread.sleep(1);
        }
    }
}
