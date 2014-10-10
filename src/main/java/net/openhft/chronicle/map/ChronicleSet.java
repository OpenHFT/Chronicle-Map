package net.openhft.chronicle.map;

import java.io.Closeable;
import java.util.Set;

public interface ChronicleSet<E> extends Set<E>, Closeable {
    public long longSize();
}
