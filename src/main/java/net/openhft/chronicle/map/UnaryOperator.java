package net.openhft.chronicle.map;

import java.io.Serializable;

/**
 * Represents a mutator that accepts one mutable argument, which it may alter and produces a result.
 *
 * <p>This is not functional as it can alter an argument.
 *
 * @param <T> the type of the mutable input

 */

public interface UnaryOperator<T> extends Serializable {
    /**
     * Applies this mutator to the given mutable argument.
     *
     * @param t the mutator argument
     * @return the  result
     */
    T update(T t);
}
