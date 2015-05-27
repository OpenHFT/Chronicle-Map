/*
 * Copyright 2015 Higher Frequency Trading
 *
 *  http://www.higherfrequencytrading.com
 *  
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.openhft.chronicle.map.impl;

import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.chronicle.map.ReplicatedChronicleMap;

public interface ReplicatedChronicleMapHolder<K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
        V, VI, MVI extends MetaBytesInterop<V, ? super VI>, R>
        extends VanillaChronicleMapHolder<K, KI, MKI, V, VI, MVI, R> {

    @Override
    ReplicatedChronicleMap<K, KI, MKI, V, VI, MVI, R> m();
}
