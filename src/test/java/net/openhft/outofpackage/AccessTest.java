package net.openhft.outofpackage;

import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.Replica;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Rob Austin.
 */
public class AccessTest {

    @Test
    public void testBootStrapTimeStampHasAccessOutOfPackage() throws Exception {
        TcpTransportAndNetworkConfig map1Config = TcpTransportAndNetworkConfig.of(8068, Arrays.asList(new InetSocketAddress("localhost", 8067)))
                .heartBeatInterval(1L, TimeUnit.SECONDS).name("map1").autoReconnectedUponDroppedConnection
                        (true);

        Map map1 = ChronicleMapBuilder
                .of(Integer.class, CharSequence.class)
                .replication((byte) 1, map1Config).create();

        final long bootStrapTimeStamp = ((Replica) map1).acquireModificationIterator((byte) 1)
                .bootStrapTimeStamp();

        Assert.assertTrue(bootStrapTimeStamp > 0);
    }
}
