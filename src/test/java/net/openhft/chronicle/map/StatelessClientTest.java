package net.openhft.chronicle.map;

import org.junit.Test;
import org.testng.Assert;

import java.io.IOException;
import java.net.InetSocketAddress;

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


}

