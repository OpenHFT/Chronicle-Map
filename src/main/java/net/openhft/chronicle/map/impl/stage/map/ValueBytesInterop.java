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

import net.openhft.chronicle.hash.impl.stage.hash.ThreadLocalCopiesHolder;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class ValueBytesInterop<V, VI, MVI extends MetaBytesInterop<V, ? super VI>> {
    @StageRef VanillaChronicleMapHolder<?, ?, ?, V, VI, MVI, ?> mh;
    @StageRef ThreadLocalCopiesHolder ch;
    
    public final BytesReader<V> valueReader =
            mh.m().valueReaderProvider.get(ch.copies, mh.m().originalValueReader);
    public final VI valueInterop =
            mh.m().valueInteropProvider.get(ch.copies, mh.m().originalValueInterop);
    
    public MVI valueMetaInterop(V value) {
        return mh.m().metaValueInteropProvider.get(
                ch.copies, mh.m().originalMetaValueInterop, valueInterop, value);
    }
}
