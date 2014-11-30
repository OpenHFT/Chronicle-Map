/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.threadlocal.Provider;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import org.jetbrains.annotations.Nullable;

final class ReaderWithSize<T> {
    private final SizeMarshaller sizeMarshaller;
    private final BytesReader<T> originalReader;
    private final Provider<BytesReader<T>> readerProvider;
    ReaderWithSize(SerializationBuilder<T> serializationBuilder) {
        sizeMarshaller = serializationBuilder.sizeMarshaller();
        originalReader = serializationBuilder.reader();
        readerProvider = (Provider<BytesReader<T>>) Provider.of(originalReader.getClass());
    }

    public ThreadLocalCopies getCopies(ThreadLocalCopies copies) {
        return readerProvider.getCopies(copies);
    }

    public T read(Bytes in, @Nullable ThreadLocalCopies copies) {
        long size = sizeMarshaller.readSize(in);
        copies = readerProvider.getCopies(copies);
        BytesReader<T> reader = readerProvider.get(copies, originalReader);
        return reader.read(in, size);
    }

    public T readNullable(Bytes in, @Nullable ThreadLocalCopies copies) {
        if (in.readBoolean())
            return null;
        return read(in, copies);
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
