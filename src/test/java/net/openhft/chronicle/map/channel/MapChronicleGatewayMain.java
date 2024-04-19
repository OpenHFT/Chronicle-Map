package net.openhft.chronicle.map.channel;

import net.openhft.chronicle.wire.channel.ChronicleGatewayMain;
import net.openhft.chronicle.wire.channel.SystemContext;
import net.openhft.chronicle.wire.channel.impl.SocketRegistry;

import java.io.IOException;

public class MapChronicleGatewayMain extends ChronicleGatewayMain {
    private MapChronicleGatewayMain(String url, SocketRegistry socketRegistry) {
        super(url, socketRegistry, SystemContext.INSTANCE);
    }

    public static void main(String... args) throws IOException {
        ChronicleGatewayMain.main();
    }
}
