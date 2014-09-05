/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import java.io.Closeable;
import java.io.File;
import java.util.concurrent.ConcurrentMap;

public interface ChronicleMap<K, V> extends ConcurrentMap<K, V>, Closeable {
    /**
     * @return size as a long.
     * @see java.util.Map size()
     */
    long longSize();

    /**
     * Get a value for a key if available.  If the value is Byteable, it will be assigned to
     * reference the value, instead of copying the data for zero copy access to the collection.
     *
     * @param key   to lookup.
     * @param value to reuse if possible. If null, a new object will be created.
     * @return value found or null if not.
     */
    V getUsing(K key, V value);

    /**
     * Acquire a value for a key, creating if absent. If the value is Byteable, it will be assigned
     * to reference the value, instead of copying the data.
     *
     * @param key   to lookup.
     * @param value to reuse if possible. If null, a new object will be created.
     * @return value created or found.
     */
    V acquireUsing(K key, V value);

    /**
     * @return The file or directory for this ChronicleMap
     */
    File file();

}
