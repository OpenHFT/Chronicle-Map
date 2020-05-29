package net.openhft.chronicle.core.io;

import net.openhft.chronicle.core.Jvm;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Collection;

public interface Closeable extends java.io.Closeable, QueryCloseable {

    static void closeQuietly(@NotNull Object... closeables) {
        closeQuietly((Object) closeables);
    }

    static void closeQuietly(@Nullable Object o) {
        if (o instanceof Collection) {
            ((Collection) o).forEach(Closeable::closeQuietly);
        } else if (o instanceof Object[]) {
            for (Object o2 : (Object[]) o) {
                closeQuietly(o2);
            }
        } else if (o instanceof java.io.Closeable) {
            try {
                ((java.io.Closeable) o).close();
            } catch (IOException | IllegalStateException e) {
                Jvm.debug().on(Closeable.class, e);
            }
        }
    }

    /**
     * Doesn't throw a checked exception.
     */
    @Override
    void close();

    @Deprecated
    default void notifyClosing() {
        // take an action before everything else closes.
    }

    @Override
    default boolean isClosed() {
        throw new UnsupportedOperationException("todo");
    }
}
