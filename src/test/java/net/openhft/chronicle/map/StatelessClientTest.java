package net.openhft.chronicle.map;

import org.junit.Ignore;
import org.junit.Test;
import org.testng.Assert;

import java.io.IOException;
import java.net.InetSocketAddress;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.chronicle.map.Builder.getPersistenceFile;
import static net.openhft.chronicle.map.TcpReplicationConfig.of;

/**
 * @author Rob Austin.
 */
public class StatelessClientTest {


    @Test
    @Ignore
    public void test() throws IOException, InterruptedException {

        ChronicleMapBuilder<Integer, CharSequence> builder =
                ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                        .entries(20000L)
                        .replicators((byte) 2, of(8076).heartBeatInterval(1L, SECONDS));
        {
            final ChronicleMap<Integer, CharSequence> map2a =
                    builder.create(getPersistenceFile());
            map2a.put(10, "EXAMPLE-10");  // this will be the last time that map1 go an update from map2
        }

        KeyValueSerializer keyValueSerializer = new KeyValueSerializer(builder.keyBuilder, builder.valueBuilder);

        InetSocketAddress remote = new InetSocketAddress("localhost", 8076);
        StatelessMapClient<Integer, String> map = new StatelessMapClient<Integer, String>(keyValueSerializer, remote);

        Assert.assertEquals("EXAMPLE-10", map.get(10));
        Assert.assertEquals(1, map.size());
    }
}

