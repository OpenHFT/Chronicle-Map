package net.openhft.chronicle.hash.function;

import java.io.Serializable;
import java.util.function.Function;

/**
 * {@link Function} + {@link Serializable}
 */
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {
}
