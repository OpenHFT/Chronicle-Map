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
