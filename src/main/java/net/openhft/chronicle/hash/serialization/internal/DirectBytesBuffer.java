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
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.serialization.BytesMarshaller;
import net.openhft.lang.io.serialization.JDKObjectSerializer;
import net.openhft.lang.threadlocal.StatefulCopyable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

class DirectBytesBuffer
        implements StatefulCopyable<DirectBytesBuffer>, Serializable {
    private static final long serialVersionUID = 0L;
    private final Serializable identity;
    transient DirectBytes buffer;

    transient ForBytesMarshaller forBytesMarshaller;
    transient ForBytesWriter forBytesWriter;
    transient ForDataValueWriter forDataValueWriter;

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
        forDataValueWriter = new ForDataValueWriter();
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

    class ForBytesMarshaller<E, M extends BytesMarshaller<E>>
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

    class ForBytesWriter<E, W extends BytesWriter<E>>
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

    class ForDataValueWriter<E> extends DataValueMetaBytesInterop<E> {

        private ForDataValueWriter() {
            super(DirectBytesBuffer.this);
        }
    }
}
