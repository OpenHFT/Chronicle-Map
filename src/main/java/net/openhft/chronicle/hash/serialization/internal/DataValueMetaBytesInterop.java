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

package net.openhft.chronicle.hash.serialization.internal;

import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.*;
import net.openhft.lang.threadlocal.ThreadLocalCopies;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

public abstract class DataValueMetaBytesInterop<E>
        extends BasicCopyingMetaBytesInterop<E, BytesWriter<E>> {
    private static final long serialVersionUID = 0L;

    protected DataValueMetaBytesInterop(DirectBytesBuffer buffer) {
        super(buffer);
    }

    void init(BytesWriter<E> writer, E e, long size) {
        Bytes buffer = this.buffer.obtain(size, false);
        writer.write(buffer, e);
        buffer.flip();
        assert buffer.remaining() == size;
        this.size = size;
        hash = 0L;
    }

    public static <E> MetaBytesInterop<E, BytesWriter<E>> forIdentity(Serializable bufferIdentity) {
        return new DirectBytesBuffer(bufferIdentity).forDataValueWriter;
    }

    public static <E> MetaProvider<E, BytesWriter<E>, MetaBytesInterop<E, BytesWriter<E>>>
    interopProvider(Class<E> eClass) {
        return new InteropProvider<>(eClass);
    }

    private static class InteropProvider<E> extends BasicCopyingMetaBytesInteropProvider<
                E, BytesWriter<E>, MetaBytesInterop<E, BytesWriter<E>>> {
        private static final long serialVersionUID = 0L;

        private final Class<E> eClass;
        transient long size;
        transient MetaBytesInterop metaByteableInterop;

        private InteropProvider(Class<E> eClass) {
            this.eClass = eClass;
            initTransients();
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            initTransients();
        }

        private void initTransients() {
            DataValueModel<E> model = DataValueModels.acquireModel(eClass);
            size = DataValueGenerator.computeNonScalarOffset(model, eClass);
            // TODO make this pick up configured deserializationFactory somehow
            metaByteableInterop = ByteableMarshaller.of(
                    (Class) DataValueClasses.directClassFor(eClass));
        }

        @Override
        public MetaBytesInterop<E, BytesWriter<E>> get(
                ThreadLocalCopies copies, MetaBytesInterop<E, BytesWriter<E>> originalMetaWriter,
                BytesWriter<E> writer, E e) {
            if (e instanceof Byteable)
                return metaByteableInterop;
            DirectBytesBuffer.ForDataValueWriter forDataValueWriter =
                    provider.get(copies, ((DataValueMetaBytesInterop)originalMetaWriter).buffer)
                            .forDataValueWriter;
            forDataValueWriter.init(writer, e, size);
            return forDataValueWriter;
        }
    }
}
