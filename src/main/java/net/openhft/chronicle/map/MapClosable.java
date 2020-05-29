package net.openhft.chronicle.map;

import net.openhft.chronicle.core.io.Closeable;

public interface MapClosable extends Closeable {

    @Override
    default boolean isClosed() {
        throw new UnsupportedOperationException();
    }
}
