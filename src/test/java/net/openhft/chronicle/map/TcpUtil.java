package net.openhft.chronicle.map;

import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;

/**
 * Created by peter on 20/05/15.
 */
public enum TcpUtil {
    ;

    @NotNull
    public static InetSocketAddress localPort(int port) {
        return new InetSocketAddress("localhost", port);
    }
}
