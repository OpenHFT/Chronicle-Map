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
