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

package net.openhft.chronicle.hash.impl.stage.hash;

import net.openhft.chronicle.hash.impl.VanillaChronicleHashHolder;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class KeyBytesInterop<K, KI, MKI extends MetaBytesInterop<K, ? super KI>> {
    @StageRef
    VanillaChronicleHashHolder<K, KI, MKI> hh;
    @StageRef ThreadLocalCopiesHolder ch;

    public final BytesReader<K> keyReader =
            hh.h().keyReaderProvider.get(ch.copies, hh.h().originalKeyReader);
    public final KI keyInterop = hh.h().keyInteropProvider.get(ch.copies, hh.h().originalKeyInterop);
    
    public MKI keyMetaInterop(K key) {
        return hh.h().metaKeyInteropProvider.get(
                ch.copies, hh.h().originalMetaKeyInterop, keyInterop, key);
    }
}
