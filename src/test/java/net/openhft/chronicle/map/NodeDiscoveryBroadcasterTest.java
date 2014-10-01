package net.openhft.chronicle.map;

import junit.framework.TestCase;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class NodeDiscoveryBroadcasterTest extends TestCase {

    public void test() {
        // test to stop it erroring with no test
    }

    private ChronicleMap<Integer, CharSequence> map1;
    private ChronicleMap<Integer, CharSequence> map2;
    private volatile byte proposedIdentifier = 1;
    private volatile boolean useAnotherIdentifer = false;





    @Test
    @Ignore
    public void test2() throws IOException, InterruptedException {

        DiscoveryCluster discoveryCluster = new DiscoveryCluster();

        ChronicleMap<Integer, CharSequence> map = discoveryCluster.discoverMap(1025, 8086);
    }




}
