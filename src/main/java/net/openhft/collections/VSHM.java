/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.collections;

import net.openhft.chronicle.map.ChronicleMap;

import java.io.File;
import java.io.IOException;

/**
 * @author Rob Austin.
 */
class ChronicleMap0<K, V> extends VanillaSharedMap<K, V> implements ChronicleMap<K, V> {
    public ChronicleMap0(ChronicleMapBuilder0<K, V> builder, File file, Class<K> kClass, Class<V> vClass)
            throws IOException {
        super(builder, file, kClass, vClass);
    }


}
