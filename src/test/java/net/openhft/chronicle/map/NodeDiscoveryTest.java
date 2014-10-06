package net.openhft.chronicle.map;

import org.junit.Ignore;
import org.junit.Test;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

public class NodeDiscoveryTest {

    @Test
    @Ignore
    public void testDiscoverMap() throws Exception {
        final NodeDiscovery nodeDiscovery = new NodeDiscovery();


        NetworkInterface networkInterface = ConcurrentExpiryMap.defaultNetworkInterface();
        Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();

        inetAddresses.nextElement();
        AddressAndPort ourAddressAndPort1 = new AddressAndPort(inetAddresses.nextElement().getAddress(),
                (short) 1237);

        final ChronicleMap<Integer, CharSequence> map = nodeDiscovery.discoverMap(8123, ourAddressAndPort1);
        Thread.sleep(1000000);
    }


}