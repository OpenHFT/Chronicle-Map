package net.openhft.chronicle.map;

import org.junit.Ignore;
import org.junit.Test;

public class NodeDiscoveryTest {

    @Test
    @Ignore
    public void testDiscoverMap() throws Exception {



        final NodeDiscovery nodeDiscovery = new NodeDiscovery();

        final ChronicleMap<Integer, CharSequence> map = nodeDiscovery.discoverMap();

        Thread.sleep(1000000);
    }


}