package net.openhft.chronicle.map;

import net.openhft.chronicle.core.io.ManagedCloseable;

public interface MapClosable extends ManagedCloseable {

    @Override
    default boolean isClosed() {
        // if we don't know, pretend it is not.
        return false;
    }
}
