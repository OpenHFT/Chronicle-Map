/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import java.util.AbstractMap;

class WriteThroughEntry<K, V> extends AbstractMap.SimpleEntry<K, V> {
    private static final long serialVersionUID = 0L;

    private final ChronicleMap<K, V> map;

    public WriteThroughEntry(ChronicleMap<K, V> map, K key, V value) {
        super(key, value);
        this.map = map;
    }

    @Override
    public V setValue(V value) {
        map.put(getKey(), value);
        return super.setValue(value);
    }
}
