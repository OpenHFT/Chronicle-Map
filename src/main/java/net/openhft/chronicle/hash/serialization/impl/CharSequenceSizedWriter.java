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

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import org.jetbrains.annotations.NotNull;

/**
 * @deprecated use one of {@link AbstractCharSequenceUtf8DataAccess} subclasses instead. This class
 * is not removed, because users who created a chronicleMap with older version of the library with
 * this class as the key or value writer should be able to read and access the map with the present
 * version of the library.
 */
@Deprecated
@SuppressWarnings("deprecation")
public final class CharSequenceSizedWriter
        implements SizedWriter<CharSequence>, EnumMarshallable<CharSequenceSizedWriter> {
    public static final CharSequenceSizedWriter INSTANCE = new CharSequenceSizedWriter();

    private CharSequenceSizedWriter() {
    }

    @Override
    public long size(@NotNull CharSequence toWrite) {
        return BytesUtil.utf8Length(toWrite);
    }

    @Override
    public void write(@NotNull Bytes out, long size, @NotNull CharSequence toWrite) {
        BytesUtil.appendUtf8(out, toWrite);
    }

    @Override
    public CharSequenceSizedWriter readResolve() {
        return INSTANCE;
    }
}
