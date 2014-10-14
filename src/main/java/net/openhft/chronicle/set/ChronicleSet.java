package net.openhft.chronicle.set;

import net.openhft.chronicle.ChronicleHash;

import java.io.Closeable;
import java.util.Set;

public interface ChronicleSet<E> extends Set<E>, ChronicleHash {
    public long longSize();
}
