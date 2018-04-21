/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map.example;

import net.openhft.chronicle.map.*;

public class HashQueryContextExamples {

    public static <K, V> void incorrectRemovePresentEntry(ChronicleMap<K, V> map, K key) {
        try (ExternalMapQueryContext<K, V, ?> q = map.queryContext(key)) {
            // q.entry(), checks if the entry is present in the map, and acquires
            // the read lock for that.
            MapEntry<K, V> entry = q.entry();
            if (entry != null) {
                // Tries to acquire the write lock to perform modification,
                // but this is an illegal upgrade: read -> write, throws IllegalMonitorStateException
                q.remove(entry);
            }
        }
    }

    public static <K, V> void correctRemovePresentEntry(ChronicleMap<K, V> map, K key) {
        try (ExternalMapQueryContext<K, V, ?> q = map.queryContext(key)) {
            q.updateLock().lock(); // acquire the update lock before checking the entry presence.
            MapEntry<K, V> entry = q.entry();
            if (entry != null)
                q.remove(entry);
        }
    }

    public static <K, V, R> void possibleAcquireUsingImplementation(
            MapQueryContext<K, V, R> q, ReturnValue<V> returnValue) {
        // For acquireUsing(), it is assumed to be very probable, that the entry is already
        // present in the map, so we will perform the whole acquireUsing() without exclusive locking
        if (q.readLock().tryLock()) {
            MapEntry<K, V> entry = q.entry();
            if (entry != null) {
                // Entry is present, return
                returnValue.returnValue(entry.value());
                return;
            }
            // Key is absent
            // Need to unlock, to lock to update lock later. Direct upgrade is forbidden.
            q.readLock().unlock();
        }
        // We are here, either if we:
        // 1) Failed to acquire the read lock, this means some other thread is holding the write
        // lock now, in this case waiting for the update lock acquisition is no longer, than for
        // the read
        // 2) Seen the entry is absent under the read lock. This means we need to insert
        // the default value into the map. that requires update-level access as well
        q.updateLock().lock();
        MapEntry<K, V> entry = q.entry();
        if (entry != null) {
            // Entry is present, return
            returnValue.returnValue(entry.value());
            return;
        }
        // Key is absent
        q.insert(q.absentEntry(), q.defaultValue(q.absentEntry()));
        returnValue.returnValue(q.entry().value());
    }

    <K> Point movePoint(ChronicleMap<K, Point> map, K key, double xMove, double yMove,
                        Point using) {
        // Moves existing point by [xMove, yMove], if absent - assumes the default point is [0, 0].
        // Returns the resulting point
        try (ExternalMapQueryContext<K, Point, ?> q = map.queryContext(key)) {
            Point offHeapPoint;
            q.updateLock().lock();
            MapEntry<K, Point> entry = q.entry();
            if (entry != null) {
                // Key is present
                offHeapPoint = entry.value().getUsing(using);
            } else {
                // Key is absent
                q.insert(q.absentEntry(), q.defaultValue(q.absentEntry()));
                offHeapPoint = q.entry().value().getUsing(using);
            }
            offHeapPoint.addX(xMove);
            offHeapPoint.addY(yMove);
            return offHeapPoint;
        }
    }

    interface Point {
        double getX();

        void setX(double x);

        double addX(double xAdd);

        double getY();

        void setY(double y);

        double addY(double yAdd);
    }
}
