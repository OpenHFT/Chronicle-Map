/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
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

package net.openhft.chronicle.hash.serialization.internal;

import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.lang.MemoryUnit;
import net.openhft.lang.io.serialization.BytesMarshaller;
import net.openhft.lang.threadlocal.ThreadLocalCopies;

import java.io.Serializable;

public abstract class CopyingMetaBytesInterop<E, W> extends BasicCopyingMetaBytesInterop<E, W> {
    private static final long serialVersionUID = 1L;

    private static final long MAX_REASONABLE_SERIALIZED_SIZE = MemoryUnit.MEGABYTES.toBytes(512L);

    public static void checkMaxSizeStillReasonable(long maxSize, Exception ex) {
        if (maxSize > MAX_REASONABLE_SERIALIZED_SIZE) {
            throw new IllegalStateException("We try to figure out size of objects " +
                    "in serialized form, but it exceeds " +
                    MAX_REASONABLE_SERIALIZED_SIZE + " bytes. We assume this is " +
                    "a error and throw exception at this point. If you really " +
                    "want larger keys/values, use ChronicleMapBuilder." +
                    "keySize(int)/valueSize(int)/entrySize(int) configurations", ex);
        }
    }

    transient W writer;
    transient E cur;

    protected CopyingMetaBytesInterop(DirectBytesBuffer buffer) {
        super(buffer);
    }

    @Override
    public <I2> boolean equivalent(
            W interop, E e, MetaBytesInterop<E, I2> otherMetaInterop, I2 otherInterop, E other) {
        return otherMetaInterop.size(otherInterop, other) == size(interop, e) &&
                otherMetaInterop.startsWith(otherInterop, buffer.buffer, other);
    }

    DirectBytesBuffer buffer() {
        return buffer;
    }

    public static <E, M extends BytesMarshaller<E>>
    MetaBytesInterop<E, M> forBytesMarshaller(Serializable bufferIdentity) {
        return new DirectBytesBuffer(bufferIdentity).forBytesMarshaller;
    }

    public static <E, W extends BytesWriter<E>>
    MetaBytesInterop<E, W> forBytesWriter(Serializable bufferIdentity) {
        return new DirectBytesBuffer(bufferIdentity).forBytesWriter;
    }

    public static <E, M extends BytesMarshaller<E>>
    MetaProvider<E, M, CopyingMetaBytesInterop<E, M>> providerForBytesMarshaller(boolean mutable,
                                                                                 long maxSize) {
        return new BytesMarshallerCopyingMetaBytesInteropProvider<>(mutable, maxSize);
    }

    private static class BytesMarshallerCopyingMetaBytesInteropProvider<E,
            M extends BytesMarshaller<E>>
            extends BasicCopyingMetaBytesInteropProvider<E, M, CopyingMetaBytesInterop<E, M>> {
        private static final long serialVersionUID = 0L;
        private final boolean mutable;
        private final long maxSize;

        public BytesMarshallerCopyingMetaBytesInteropProvider(boolean mutable, long maxSize) {
            this.mutable = mutable;
            this.maxSize = maxSize;
        }

        @Override
        public CopyingMetaBytesInterop<E, M> get(
                ThreadLocalCopies copies,
                CopyingMetaBytesInterop<E, M> originalMetaWriter, M writer, E e) {
            DirectBytesBuffer.ForBytesMarshaller forBytesMarshaller =
                    provider.get(copies, originalMetaWriter.buffer()).forBytesMarshaller;
            forBytesMarshaller.init(writer, e, mutable, maxSize);
            return forBytesMarshaller;
        }
    }

    public static <E, W extends BytesWriter<E>>
    MetaProvider<E, W, CopyingMetaBytesInterop<E, W>> providerForBytesWriter(boolean mutable) {
        return new BytesWriterCopyingMetaBytesInteropProvider<>(mutable);
    }

    private static class BytesWriterCopyingMetaBytesInteropProvider<E, W extends BytesWriter<E>>
            extends BasicCopyingMetaBytesInteropProvider<E, W, CopyingMetaBytesInterop<E, W>> {
        private static final long serialVersionUID = 0L;
        private final boolean mutable;

        public BytesWriterCopyingMetaBytesInteropProvider(boolean mutable) {
            this.mutable = mutable;
        }

        @Override
        public CopyingMetaBytesInterop<E, W> get(ThreadLocalCopies copies,
                CopyingMetaBytesInterop<E, W> originalMetaWriter, W writer, E e) {
            DirectBytesBuffer.ForBytesWriter forBytesWriter =
                    provider.get(copies, originalMetaWriter.buffer()).forBytesWriter;
            forBytesWriter.init(writer, e, mutable);
            return forBytesWriter;
        }
    }
}
