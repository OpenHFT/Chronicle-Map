/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

/**
 * Internal helpers shared by Chronicle-Map {@code MapMethods}.
 * <p>
 * The methods here encapsulate common patterns for reading the current
 * value associated with a key under the appropriate locks, minimising
 * duplication and keeping locking rules in one place.
 */
final class MapMethodsSupport {

    private MapMethodsSupport() {
    }

    /**
     * Returns the current value via the supplied {@code returnValue} if the
     * entry is present in the supplied query context.
     */
    static <V> void returnCurrentValueIfPresent(
            MapQueryContext<?, V, ?> q, ReturnValue<V> returnValue) {
        MapEntry<?, V> entry = q.entry();
        if (entry != null)
            returnValue.returnValue(entry.value());
    }

    /**
     * Attempts to return the current value using a non-blocking read lock
     * first and, if necessary, falling back to taking the update lock.
     * <p>
     * This allows callers to avoid unnecessary upgrades when the key is
     * absent while still guaranteeing a consistent view when it is present.
     *
     * @return {@code true} if a value was found and returned, {@code false}
     * if the key was absent
     */
    static <V> boolean tryReturnCurrentValueIfPresent(
            MapQueryContext<?, V, ?> q, ReturnValue<V> returnValue) {
        if (q.readLock().tryLock()) {
            MapEntry<?, V> entry = q.entry();
            if (entry != null) {
                returnValue.returnValue(entry.value());
                return true;
            }
            // Key is absent
            q.readLock().unlock();
        }
        q.updateLock().lock();
        MapEntry<?, V> entry = q.entry();
        if (entry != null) {
            returnValue.returnValue(entry.value());
            return true;
        }
        return false;
    }
}
