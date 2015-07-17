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

package net.openhft.chronicle.map.impl.stage.map;

import net.openhft.chronicle.hash.impl.stage.hash.Chaining;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ReplicatedChronicleMap;
import net.openhft.chronicle.map.impl.ReplicatedChronicleMapHolder;
import net.openhft.sg.Staged;

@Staged
public class ReplicatedChronicleMapHolderImpl<
        K, KI, MKI extends MetaBytesInterop<K, ? super KI>,
        V, VI, MVI extends MetaBytesInterop<V, ? super VI>, R
        >
        extends Chaining
        implements ReplicatedChronicleMapHolder<K, KI, MKI, V, VI, MVI, R> {

    private final ReplicatedChronicleMap<K, KI, MKI, V, VI, MVI, R> m;

    public ReplicatedChronicleMapHolderImpl(ReplicatedChronicleMap<K, KI, MKI, V, VI, MVI, R> m) {
        super();
        this.m = m;
    }

    public ReplicatedChronicleMapHolderImpl(ReplicatedChronicleMapHolderImpl c) {
        super(c);
        this.m = (ReplicatedChronicleMap<K, KI, MKI, V, VI, MVI, R>) c.m;
    }

    @Override
    public Chaining createChaining() {
        return new ReplicatedChronicleMapHolderImpl(this);
    }

    @Override
    public ReplicatedChronicleMap<K, KI, MKI, V, VI, MVI, R> m() {
        return m;
    }

    @Override
    public ChronicleMap<K, V> map() {
        return m();
    }
}
