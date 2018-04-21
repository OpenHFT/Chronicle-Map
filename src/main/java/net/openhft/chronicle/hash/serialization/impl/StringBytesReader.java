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
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link BytesReader} implementation for String, for the primary ChronicleMap's key or value type
 * {@link StringSizedReader} + {@link StringUtf8DataAccess} are more efficient (because don't
 * store the size twice), so this reader is useful in conjunction with {@link ListMarshaller} or
 * {@link SetMarshaller}.
 *
 * @see CharSequenceBytesWriter
 */
public class StringBytesReader implements BytesReader<String>, StatefulCopyable<StringBytesReader> {

    /**
     * Cache field
     */
    private transient StringBuilder sb;

    public StringBytesReader() {
        initTransients();
    }

    private void initTransients() {
        sb = new StringBuilder();
    }

    @NotNull
    @Override
    public String read(Bytes in, @Nullable String using) {
        if (in.readUtf8(sb)) {
            return sb.toString();
        } else {
            throw new NullPointerException("BytesReader couldn't read null");
        }
    }

    @Override
    public StringBytesReader copy() {
        return new StringBytesReader();
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        // no fields to read
        initTransients();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        // no fields to write
    }
}
