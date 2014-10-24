package net.openhft.chronicle.map;

import org.junit.Ignore;
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


    public static final int SIZE = 10000;

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
    @Ignore
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

        for (int i = 0; i < SIZE; i++) {
            payload.put(i, "some value=" + i);
        }


        statelessMap.putAll(payload);

        Assert.assertEquals("some value=10", statelessMap.get(10));
        Assert.assertEquals(SIZE, statelessMap.size());

        serverMap.close();
        statelessMap.close();

    }



    @Test
    public void testEquals() throws IOException, InterruptedException {

        final ChronicleMap<Integer, CharSequence> serverMap1;
        final ChronicleMap<Integer, CharSequence> serverMap2;
        final ChronicleMap<Integer, CharSequence> statelessMap1;
        final ChronicleMap<Integer, CharSequence> statelessMap2;


        // stateless client
        {
            statelessMap1 = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .stateless(remoteAddress(new InetSocketAddress("localhost", 8076))).create();
            statelessMap2 = ChronicleMapBuilder.of(Integer
                    .class, CharSequence.class)
                    .stateless(remoteAddress(new InetSocketAddress("localhost", 8077))).create();


        }

        // server
        {
            serverMap1 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .replicators((byte) 2, TcpReplicationConfig.of(8076)).create();
            serverMap2 = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                    .replicators((byte) 2, TcpReplicationConfig.of(8077)).create();

        }


        Map<Integer, CharSequence> payload = new HashMap<Integer, CharSequence>();

        for (int i = 0; i < 10; i++) {
            payload.put(i, "some value=" + i);
        }


        statelessMap1.putAll(payload);
        statelessMap2.putAll(payload);

        Assert.assertTrue(statelessMap1.equals(statelessMap2));

        Assert.assertTrue(statelessMap1.equals(payload));
        Assert.assertTrue(statelessMap2.equals(payload));

        statelessMap1.close();
        statelessMap1.close();

        serverMap1.close();
        serverMap2.close();

    }


}

