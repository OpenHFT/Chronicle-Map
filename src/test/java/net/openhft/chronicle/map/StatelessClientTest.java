package net.openhft.chronicle.map;

import org.junit.Test;
import org.testng.Assert;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import static net.openhft.chronicle.common.StatelessBuilder.remoteAddress;


/**
 * @author Rob Austin.
 */
public class StatelessClientTest {


    @Test
    public void test() throws IOException, InterruptedException {

        final ChronicleMap<Integer, CharSequence> serverMap;
        final ChronicleMap<Integer, CharSequence> statelessMap;

// server
        {
            serverMap = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .replicators((byte) 2, TcpReplicationConfig.of(8076)).create();

            serverMap.put(10, "EXAMPLE-10");
        }


// stateless client
        {
            statelessMap = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .stateless(remoteAddress(new InetSocketAddress("localhost", 8076))).create();

            Assert.assertEquals("EXAMPLE-10", statelessMap.get(10));
            Assert.assertEquals(1, statelessMap.size());
        }

        serverMap.close();
        statelessMap.close();

    }


    @Test
    public void testClientCreatedBeforeServer() throws IOException, InterruptedException {

        final ChronicleMap<Integer, CharSequence> serverMap;
        final ChronicleMap<Integer, CharSequence> statelessMap;


        // stateless client
        {
            statelessMap = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .stateless(remoteAddress(new InetSocketAddress("localhost", 8076))).create();


        }

        // server
        {
            serverMap = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .replicators((byte) 2, TcpReplicationConfig.of(8076)).create();

            serverMap.put(10, "EXAMPLE-10");
        }


        Assert.assertEquals("EXAMPLE-10", statelessMap.get(10));
        Assert.assertEquals(1, statelessMap.size());

        serverMap.close();
        statelessMap.close();

    }


    @Test
    public void testBufferOverFlowPutAll() throws IOException, InterruptedException {

        final ChronicleMap<Integer, CharSequence> serverMap;
        final ChronicleMap<Integer, CharSequence> statelessMap;


        // stateless client
        {
            statelessMap = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .stateless(remoteAddress(new InetSocketAddress("localhost", 8076))).create();


        }

        // server
        {
            serverMap = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .replicators((byte) 2, TcpReplicationConfig.of(8076)).create();

        }


        Map<Integer, CharSequence> payload = new HashMap<Integer, CharSequence>();

        for (int i = 0; i < 100000; i++) {
            serverMap.put(i, "some value=" + i);
        }


        statelessMap.putAll(payload);

        Assert.assertEquals("some value=10", statelessMap.get(10));
        Assert.assertEquals(100000, statelessMap.size());

        serverMap.close();
        statelessMap.close();

    }
}

