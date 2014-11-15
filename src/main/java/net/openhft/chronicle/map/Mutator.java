package net.openhft.chronicle.map;

import java.io.Serializable;

/**
 * Represents a mutator that accepts one mutable argument, which it may alter and produces a result.
 *
 * <p>This is not functional as it can alter an argument.
 *
 * @param <T> the type of the mutable input
 * @param <R> the type of the result of the mutator
 */

public interface Mutator<T, R> extends Serializable {
    /**
     * Applies this mutator to the given mutable argument.
     *
     * @param t the mutator argument
     * @return the  result
     */
    R update(T t);
}
