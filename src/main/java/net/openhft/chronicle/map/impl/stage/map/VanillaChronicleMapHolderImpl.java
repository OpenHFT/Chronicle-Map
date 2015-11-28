/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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
import net.openhft.chronicle.map.VanillaChronicleMap;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.sg.Staged;

@Staged
public class VanillaChronicleMapHolderImpl<K, V, R> extends Chaining
        implements VanillaChronicleMapHolder<K, V, R> {
    
    private final VanillaChronicleMap<K, V, R> m;

    public VanillaChronicleMapHolderImpl(VanillaChronicleMap<K, V, R> m) {
        super();
        this.m = m;
    }
    
    public VanillaChronicleMapHolderImpl(
            ChainingInterface c, VanillaChronicleMap<K, V, R> m) {
        super(c);
        this.m = m;
    }

    @Override
    public VanillaChronicleMap<K, V, R> m() {
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

    public ChronicleHash<K, ?, ?, ?> hash() {
        return set() != null ? set() : map();
    }
}
