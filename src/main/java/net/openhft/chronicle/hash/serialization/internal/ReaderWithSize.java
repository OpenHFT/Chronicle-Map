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

package net.openhft.chronicle.hash.serialization.internal;

import net.openhft.chronicle.hash.serialization.internal.SerializationBuilder;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.threadlocal.Provider;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import org.jetbrains.annotations.Nullable;

public final class ReaderWithSize<T> {
    private final SizeMarshaller sizeMarshaller;
    private final BytesReader<T> originalReader;
    private final Provider<BytesReader<T>> readerProvider;

    public ReaderWithSize(SerializationBuilder<T> serializationBuilder) {
        sizeMarshaller = serializationBuilder.sizeMarshaller();
        originalReader = serializationBuilder.reader();
        readerProvider = (Provider<BytesReader<T>>) Provider.of(originalReader.getClass());
    }

    public ThreadLocalCopies getCopies(ThreadLocalCopies copies) {
        return readerProvider.getCopies(copies);
    }

    public T read(Bytes in, @Nullable ThreadLocalCopies copies, @Nullable T using) {
        long size = sizeMarshaller.readSize(in);
        copies = readerProvider.getCopies(copies);
        BytesReader<T> reader = readerProvider.get(copies, originalReader);
        return reader.read(in, size, using);
    }

    public T readNullable(Bytes in, @Nullable ThreadLocalCopies copies, T using) {
        if (in.readBoolean())
            return null;
        return read(in, copies, using);
    }

    public BytesReader<T> readerForLoop(@Nullable ThreadLocalCopies copies) {
        copies = readerProvider.getCopies(copies);
        return readerProvider.get(copies, originalReader);
    }

    public T readInLoop(Bytes in, BytesReader<T> reader) {
        long size = sizeMarshaller.readSize(in);
        return reader.read(in, size);
    }

    public T readNullableInLoop(Bytes in, BytesReader<T> reader) {
        if (in.readBoolean())
            return null;
        return readInLoop(in, reader);
    }
}
