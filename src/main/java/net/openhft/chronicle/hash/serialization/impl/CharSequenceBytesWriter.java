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
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.ListMarshaller;
import net.openhft.chronicle.hash.serialization.SetMarshaller;
import org.jetbrains.annotations.NotNull;

/**
 * {@link BytesWriter} implementation for {@link CharSequence}, for the primary ChronicleMap's key
 * or value type {@link CharSequenceUtf8DataAccess} + {@link CharSequenceSizedReader} are more
 * efficient (because don't store the size twice), so this writer is useful in conjunction with
 * {@link ListMarshaller} or {@link SetMarshaller}.
 *
 * @see StringBytesReader
 */
public final class CharSequenceBytesWriter
        implements BytesWriter<CharSequence>, EnumMarshallable<CharSequenceBytesWriter> {
    public static final CharSequenceBytesWriter INSTANCE = new CharSequenceBytesWriter();

    private CharSequenceBytesWriter() {
    }

    @Override
    public void write(Bytes out, @NotNull CharSequence toWrite) {
        if (toWrite == null)
            throw new NullPointerException("BytesWriter couldn't write null");
        out.writeUtf8(toWrite);
    }

    @Override
    public CharSequenceBytesWriter readResolve() {
        return INSTANCE;
    }
}
