/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import java.io.Serializable;

/**
 * Default value computation strategy, used
 * in {@link ChronicleMapBuilder#defaultValueProvider(DefaultValueProvider)} configuration.
 *
 * @param <K> map key class
 * @param <V> map value class
 */
public interface DefaultValueProvider<K, V> extends Serializable {
    /**
     * Returns a value to be put during {@link ChronicleMap#acquireUsing(Object, Object)} call
     * for the specified key, if it is absent in the map.
     *
     * @param key key absent in the map
     * @return value to be put for the specified key in the map
     */
    V get(K key);
}
