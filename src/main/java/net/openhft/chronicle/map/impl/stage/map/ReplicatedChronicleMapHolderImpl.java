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
