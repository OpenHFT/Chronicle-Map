package net.openhft.chronicle;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * @author Rob Austin.
 */
public class StatelessBuilder {

    private long timeoutMs = TimeUnit.SECONDS.toMillis(5);

    private InetSocketAddress remoteAddress;

    public long timeoutMs() {
        return timeoutMs;
    }

    public void timeout(long timeout, TimeUnit units) {
        this.timeoutMs = units.toMillis(timeout);
    }

    private StatelessBuilder(InetSocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    public static StatelessBuilder remoteAddress(InetSocketAddress inetSocketAddress) {
        return new StatelessBuilder(inetSocketAddress);
    }

    public InetSocketAddress remoteAddress() {
        return remoteAddress;
    }
}
