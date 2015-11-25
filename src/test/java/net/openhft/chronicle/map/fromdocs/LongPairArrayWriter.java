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

package net.openhft.chronicle.map.fromdocs;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import net.openhft.chronicle.hash.serialization.impl.EnumMarshallable;
import org.jetbrains.annotations.NotNull;

enum LongPairArrayWriter implements SizedWriter<LongPair[]>, EnumMarshallable<LongPairArrayWriter> {
    INSTANCE;

    @Override
    public long size(@NotNull LongPair[] toWrite) {
        return toWrite.length * 16L;
    }

    @Override
    public void write(@NotNull Bytes out, long size, @NotNull LongPair[] toWrite) {
        for (LongPair pair : toWrite) {
            out.writeLong(pair.first);
            out.writeLong(pair.second);
        }
    }

    @Override
    public LongPairArrayWriter readResolve() {
        return INSTANCE;
    }
}
