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

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.SizedReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Reader of {@link Byteable} subclasses.
 *
 * @param <T> the subtype of {@link Byteable} deserialized
 */
public class ByteableSizedReader<T extends Byteable> extends InstanceCreatingMarshaller<T>
        implements SizedReader<T> {

    public ByteableSizedReader(Class<T> tClass) {
        super(tClass);
    }

    @NotNull
    @Override
    public final T read(@NotNull Bytes in, long size, @Nullable T using) {
        if (using == null)
            using = createInstance();
        using.bytesStore(in.bytesStore(), in.readPosition(), size);
        return using;
    }
}
