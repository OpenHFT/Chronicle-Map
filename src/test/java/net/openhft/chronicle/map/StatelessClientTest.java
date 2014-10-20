package net.openhft.chronicle.map;

import org.junit.Test;
import org.testng.Assert;

import java.io.IOException;
import java.net.InetSocketAddress;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.StatelessBuilder.remoteAddress;
import static net.openhft.chronicle.map.Builder.getPersistenceFile;
import static net.openhft.chronicle.map.TcpReplicationConfig.of;

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
                    .entries(20000L)
                    .replicators((byte) 2, of(8076).heartBeatInterval(1L, SECONDS))
                    .create(getPersistenceFile());

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

