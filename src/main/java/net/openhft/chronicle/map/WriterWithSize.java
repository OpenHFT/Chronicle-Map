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

import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesInterop;
import net.openhft.chronicle.hash.serialization.internal.MetaBytesWriter;
import net.openhft.chronicle.hash.serialization.internal.MetaProvider;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.threadlocal.Provider;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import org.jetbrains.annotations.Nullable;

import java.nio.BufferOverflowException;

final class WriterWithSize<T> {
    private final SizeMarshaller sizeMarshaller;
    private final Object originalWriter;
    private final Provider writerProvider;
    private final MetaBytesInterop originalMetaWriter;
    private final MetaProvider metaWriterProvider;
    private final BufferResizer bufferResizer;

    WriterWithSize(SerializationBuilder<T> serializationBuilder,
                   @Nullable BufferResizer bufferResizer) {
        this.bufferResizer = bufferResizer;
        sizeMarshaller = serializationBuilder.sizeMarshaller();
        originalWriter = serializationBuilder.interop();
        writerProvider = Provider.of((Class) originalWriter.getClass());
        originalMetaWriter = serializationBuilder.metaInterop();
        metaWriterProvider = serializationBuilder.metaInteropProvider();
    }

    ThreadLocalCopies getCopies(ThreadLocalCopies copies) {
        copies = writerProvider.getCopies(copies);
        return metaWriterProvider.getCopies(copies);
    }

    public ThreadLocalCopies write(Bytes out, T t, @Nullable ThreadLocalCopies copies) {
        copies = writerProvider.getCopies(copies);
        Object writer = writerProvider.get(copies, originalWriter);
        copies = metaWriterProvider.getCopies(copies);
        MetaBytesWriter metaWriter = metaWriterProvider.get(copies, originalMetaWriter, writer, t);
        long size = metaWriter.size(writer, t);
        if (bufferResizer != null)
            out = resizeIfRequired(out, size);
        sizeMarshaller.writeSize(out, size);
        metaWriter.write(writer, out, t);
        return copies;
    }

    private Bytes resizeIfRequired(Bytes out, long size) {
        if (out.remaining() < size + 8) {
            long newCapacity = out.remaining() + size + 8;
            if (newCapacity > Integer.MAX_VALUE)
                throw new BufferOverflowException();
             return bufferResizer.resizeBuffer((int) newCapacity);
        }
        return out;
    }


    public ThreadLocalCopies writeNullable(Bytes out, T t, @Nullable ThreadLocalCopies copies) {
        out.writeBoolean(t == null);
        if (t == null)
            return copies;
        return write(out, t, copies);
    }

    public Object writerForLoop(@Nullable ThreadLocalCopies copies) {
        copies = writerProvider.getCopies(copies);
        return writerProvider.get(copies, originalWriter);
    }

    public ThreadLocalCopies writeInLoop(Bytes out, T t, Object writer,
                                         @Nullable ThreadLocalCopies copies) {
        copies = metaWriterProvider.getCopies(copies);
        MetaBytesWriter metaWriter = metaWriterProvider.get(copies, originalMetaWriter, writer, t);
        long size = metaWriter.size(writer, t);
        if (bufferResizer != null)
            out = resizeIfRequired(out, size);
        sizeMarshaller.writeSize(out, size);
        metaWriter.write(writer, out, t);
        return copies;
    }

    public ThreadLocalCopies writeNullableInLoop(Bytes out, T t, Object writer,
                                                 @Nullable ThreadLocalCopies copies) {
        out.writeBoolean(t == null);
        if (t == null)
            return copies;
        return writeInLoop(out, t, writer, copies);
    }
}

interface BufferResizer {

    /**
     * @param newCapacity
     * @return the newly resize buffer
     */
    public Bytes resizeBuffer(int newCapacity);
}
