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

package net.openhft.chronicle.map.impl.stage.map;

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.impl.VanillaChronicleHash;
import net.openhft.chronicle.hash.impl.stage.hash.Chaining;
import net.openhft.chronicle.hash.impl.stage.hash.ChainingInterface;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ReplicatedChronicleMap;
import net.openhft.chronicle.map.VanillaChronicleMap;
import net.openhft.chronicle.map.impl.ReplicatedChronicleMapHolder;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.sg.Stage;
import net.openhft.sg.Staged;

@Staged
public abstract class ReplicatedChronicleMapHolderImpl<K, V, R>
        extends Chaining
        implements ReplicatedChronicleMapHolder<K, V, R> {

    @Stage("Map")
    private ReplicatedChronicleMap<K, V, R> m = null;

    public ReplicatedChronicleMapHolderImpl(VanillaChronicleMap map) {
        super(map);
    }

    public ReplicatedChronicleMapHolderImpl(
            ChainingInterface rootContextInThisThread, VanillaChronicleMap map) {
        super(rootContextInThisThread, map);
    }

    @Override
    public void initMap(VanillaChronicleMap map) {
        // alternative to this "unsafe" casting approach is proper generalization
        // of Chaining/ChainingInterface, but this causes issues with current version
        // of stage-compiler.
        // TODO generalize Chaining with <M extends VanillaCM> when stage-compiler is improved.
        //noinspection unchecked
        m = (ReplicatedChronicleMap<K, V, R>) map;
    }

    @Override
    public ReplicatedChronicleMap<K, V, R> m() {
        return m;
    }

    @Override
    public VanillaChronicleHash<K, ?, ?, ?> h() {
        return m;
    }

    @Override
    public ChronicleMap<K, V> map() {
        return m;
    }

    @Override
    public ChronicleSet<K> set() {
        return m.chronicleSet;
    }

    public ChronicleHash<K, ?, ?, ?> hash() {
        return set() != null ? set() : map();
    }
}
