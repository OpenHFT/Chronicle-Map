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
     * Returns a value to be put and returned from {@link ChronicleMap#get(Object)}  get()} or
     * {@link ChronicleMap#getUsing(Object, Object)} call for the specified key, if it is absent
     * in the map.
     *
     * <p>If this method returns {@code null}, it isn't put for the key in the map.
     *
     * @param key key absent in the map
     * @param usingValue an object provided as second parameter of current
     * {@link ChronicleMap#getUsing(Object, Object)} call, or {@code null}, if this method is called
     * within {@code get()} call. This object might be optionally initialized and returned back
     * from this method.
     * @return value to be put for the specified key in the map, or {@code null}, if the key should
     * remain absent in the map
     */
    V get(K key, V usingValue);
}
