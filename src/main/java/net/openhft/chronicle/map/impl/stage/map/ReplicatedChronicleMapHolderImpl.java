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
import net.openhft.chronicle.hash.impl.stage.hash.Chaining;
import net.openhft.chronicle.hash.impl.stage.hash.ChainingInterface;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ReplicatedChronicleMap;
import net.openhft.chronicle.map.impl.ReplicatedChronicleMapHolder;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.sg.Staged;

@Staged
public abstract class ReplicatedChronicleMapHolderImpl<K, V, R> extends Chaining
        implements ReplicatedChronicleMapHolder<K, V, R> {

    private final ReplicatedChronicleMap<K, V, R> m;

    public ReplicatedChronicleMapHolderImpl(ReplicatedChronicleMap<K, V, R> m) {
        super();
        this.m = m;
    }

    public ReplicatedChronicleMapHolderImpl(
            ChainingInterface c, ReplicatedChronicleMap<K, V, R> m) {
        super(c);
        this.m = m;
    }

    @Override
    public ReplicatedChronicleMap<K, V, R> m() {
        return m;
    }

    @Override
    public ChronicleMap<K, V> map() {
        return m();
    }

    @Override
    public ChronicleSet<K> set() {
        return m.chronicleSet;
    }

    @Override
    public ChronicleHash<K, ?, ?, ?> hash() {
        return set() != null ? set() : map();
    }
}
