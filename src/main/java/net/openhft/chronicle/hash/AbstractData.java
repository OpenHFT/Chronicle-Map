/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash;

/**
 * Defines reasonable defaults for {@code Data}'s {@code equals()}, {@code hashCode()} and
 * {@code toString()}. They should be default implementations in the {@code Data} interface itself,
 * but Java 8 doesn't allow to override {@code Object}'s methods by default implementations
 * in interfaces.
 *
 * @param <T> value type represented by this Data
 */
public abstract class AbstractData<T> implements Data<T> {

    /**
     * Constructor for use by subclasses.
     */
    protected AbstractData() {
    }

    /**
     * Computes value's hash code by applying a hash function to {@code Data}'s <i>bytes</i>
     * representation. Delegates to {@link #dataHashCode()}.
     */
    @Override
    public int hashCode() {
        return dataHashCode();
    }

    /**
     * Compares {@code Data}s' <i>bytes</i> representations.
     * Delegates to {@link #dataEquals(Object)}.
     */
    @Override
    public boolean equals(Object obj) {
        return dataEquals(obj);
    }

    /**
     * Delegates to {@code Data}'s <i>object</i> {@code toString()}. If deserialization fails with
     * exception (e. g. if data bytes are corrupted, and represent not a valid serialized form of
     * an object), traces the data's bytes and the exception.
     * Delegates to {@link #dataToString()}.
     */
    @Override
    public String toString() {
        return dataToString();
    }
}
