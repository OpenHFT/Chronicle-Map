package net.openhft.chronicle.common;

import java.io.Closeable;

/**
 * @author Rob Austin.
 */
public interface ClosableHolder {

    void addCloseable(Closeable closeable);

}
