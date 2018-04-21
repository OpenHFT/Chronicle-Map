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
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.ListMarshaller;
import net.openhft.chronicle.hash.serialization.SetMarshaller;
import net.openhft.chronicle.hash.serialization.StatefulCopyable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link BytesReader} implementation for {@code CharSequence}. For the primary ChronicleMap's key
 * or value type {@link CharSequenceSizedReader} + {@link CharSequenceUtf8DataAccess} are more
 * efficient (because don't store the size twice), so this reader is useful in conjunction with
 * {@link ListMarshaller} or {@link SetMarshaller}.
 *
 * @see CharSequenceBytesWriter
 */
public final class CharSequenceBytesReader implements BytesReader<CharSequence>,
        StatefulCopyable<CharSequenceBytesReader>, EnumMarshallable<CharSequenceBytesReader> {
    public static final CharSequenceBytesReader INSTANCE = new CharSequenceBytesReader();

    private CharSequenceBytesReader() {
    }

    @NotNull
    @Override
    public CharSequence read(Bytes in, @Nullable CharSequence using) {
        StringBuilder usingSB;
        if (using instanceof StringBuilder) {
            usingSB = (StringBuilder) using;
        } else {
            usingSB = new StringBuilder();
        }
        if (in.readUtf8(usingSB)) {
            return usingSB;
        } else {
            throw new NullPointerException("BytesReader couldn't read null");
        }
    }

    @Override
    public CharSequenceBytesReader copy() {
        return INSTANCE;
    }

    @Override
    public CharSequenceBytesReader readResolve() {
        return INSTANCE;
    }
}
