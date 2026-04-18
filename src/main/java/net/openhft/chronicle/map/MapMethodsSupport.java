/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

final class MapMethodsSupport {

    private MapMethodsSupport() {
    }

    static <V> void returnCurrentValueIfPresent(
            MapQueryContext<?, V, ?> q, ReturnValue<V> returnValue) {
        MapEntry<?, V> entry = q.entry();
        if (entry != null)
            returnValue.returnValue(entry.value());
    }

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
