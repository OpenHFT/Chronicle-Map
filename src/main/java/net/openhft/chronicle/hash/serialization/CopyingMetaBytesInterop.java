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

package net.openhft.chronicle.hash.serialization;

import net.openhft.lang.threadlocal.Provider;
import net.openhft.lang.threadlocal.StatefulCopyable;
import net.openhft.lang.threadlocal.ThreadLocalCopies;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.serialization.BytesMarshaller;
import net.openhft.lang.io.serialization.JDKObjectSerializer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

public abstract class CopyingMetaBytesInterop<E, W> implements MetaBytesInterop<E, W> {
    private static final long serialVersionUID = 0L;

    final DirectBytesBuffer buffer;
    transient long size;
    transient long hash;
    transient W writer;
    transient E cur;

    private CopyingMetaBytesInterop(DirectBytesBuffer buffer) {
        this.buffer = buffer;
    }

    void init(W writer, E e, boolean mutable, long maxSize) {
        if (mutable || writer != this.writer || e != cur) {
            this.writer = writer;
            cur = e;
            Bytes buffer = this.buffer.obtain(maxSize);
            innerWrite(writer, buffer, e);
            buffer.flip();
            size = buffer.remaining();
            hash = 0L;
        }
    }

    abstract void innerWrite(W writer, Bytes bytes, E e);

    @Override
    public long size(W writer, E e) {
        return size;
    }

    @Override
    public boolean startsWith(W writer, Bytes bytes, E e) {
        return bytes.startsWith(buffer.buffer);
    }

    @Override
    public long hash(W writer, E e) {
        long h;
        if ((h = hash) == 0L)
            return hash = Hasher.hash(buffer.buffer);
        return h;
    }

    @Override
    public void write(W writer, Bytes bytes, E e) {
        assert bytes.limit() == bytes.capacity();
        bytes.write(buffer.buffer);
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

    private static final Provider<DirectBytesBuffer> provider =
            Provider.of(DirectBytesBuffer.class);

    private static abstract class AbstractCopyingMetaBytesInteropProvider<E, I,
            MI extends CopyingMetaBytesInterop<E, I>>
            implements MetaProvider<E, I, MI> {
        private static final long serialVersionUID = 0L;

        @Override
        public ThreadLocalCopies getCopies(ThreadLocalCopies copies) {
            return provider.getCopies(copies);
        }
    }

    public static <E, M extends BytesMarshaller<E>>
    MetaProvider<E, M, CopyingMetaBytesInterop<E, M>> providerForBytesMarshaller(boolean mutable,
                                                                                 long maxSize) {
        return new BytesMarshallerCopyingMetaBytesInteropProvider<E, M>(mutable, maxSize);
    }

    private static class BytesMarshallerCopyingMetaBytesInteropProvider<E,
            M extends BytesMarshaller<E>>
            extends AbstractCopyingMetaBytesInteropProvider<E, M, CopyingMetaBytesInterop<E, M>> {
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
    MetaProvider<E, W, CopyingMetaBytesInterop<E, W>> providerForBytesWriter(boolean mutable,
                                                                             long maxSize) {
        return new BytesWriterCopyingMetaBytesInteropProvider<E, W>(mutable, maxSize);
    }

    private static class BytesWriterCopyingMetaBytesInteropProvider<E, W extends BytesWriter<E>>
            extends AbstractCopyingMetaBytesInteropProvider<E, W, CopyingMetaBytesInterop<E, W>> {
        private static final long serialVersionUID = 0L;
        private final boolean mutable;
        private final long maxSize;

        public BytesWriterCopyingMetaBytesInteropProvider(boolean mutable, long maxSize) {
            this.mutable = mutable;
            this.maxSize = maxSize;
        }

        @Override
        public CopyingMetaBytesInterop<E, W> get(
                ThreadLocalCopies copies,
                CopyingMetaBytesInterop<E, W> originalMetaWriter, W writer, E e) {
            DirectBytesBuffer.ForBytesWriter forBytesWriter =
                    provider.get(copies, originalMetaWriter.buffer()).forBytesWriter;
            forBytesWriter.init(writer, e, mutable, maxSize);
            return forBytesWriter;
        }
    }


    private static class DirectBytesBuffer
            implements StatefulCopyable<DirectBytesBuffer>, Serializable {
        private static final long serialVersionUID = 0L;
        private final Serializable identity;
        private transient DirectBytes buffer;

        private transient ForBytesMarshaller forBytesMarshaller;
        private transient ForBytesWriter forBytesWriter;

        DirectBytesBuffer(Serializable identity) {
            this.identity = identity;
            initTransients();
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            initTransients();
        }

        private void initTransients() {
            forBytesMarshaller = new ForBytesMarshaller();
            forBytesWriter = new ForBytesWriter();
        }

        Bytes obtain(long maxSize) {
            DirectBytes buf;
            if ((buf = buffer) != null) {
                if (maxSize <= buf.capacity()) {
                    return buf.clear();
                } else {
                    DirectStore store = (DirectStore) buf.store();
                    store.resize(maxSize, false);
                    return buffer = store.bytes();
                }
            } else {
                buffer = new DirectStore(JDKObjectSerializer.INSTANCE, maxSize, false).bytes();
                return buffer;
            }
        }

        @Override
        public Object stateIdentity() {
            return identity;
        }

        @Override
        public DirectBytesBuffer copy() {
            return new DirectBytesBuffer(identity);
        }

        private class ForBytesMarshaller<E, M extends BytesMarshaller<E>>
                extends CopyingMetaBytesInterop<E, M> {
            private static final long serialVersionUID = 0L;

            private ForBytesMarshaller() {
                super(DirectBytesBuffer.this);
            }

            @Override
            void innerWrite(M writer, Bytes bytes, E e) {
                writer.write(bytes, e);
            }
        }

        private class ForBytesWriter<E, W extends BytesWriter<E>>
                extends CopyingMetaBytesInterop<E, W> {
            private static final long serialVersionUID = 0L;

            private ForBytesWriter() {
                super(DirectBytesBuffer.this);
            }

            @Override
            void innerWrite(W writer, Bytes bytes, E e) {
                writer.write(bytes, e);
            }
        }
    }
}
