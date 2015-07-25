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

import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.serialization.BytesMarshallableSerializer;
import net.openhft.lang.io.serialization.BytesMarshaller;
import net.openhft.lang.io.serialization.ObjectSerializer;
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
    private final ObjectSerializer objectSerializer = BytesMarshallableSerializer.create();

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

    Bytes obtain(long maxSize, boolean boundsChecking) {
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
            buffer = new DirectStore(objectSerializer, Math.max(1, maxSize), true)
                    .bytes();
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

        void init(M writer, E e, boolean mutable, long maxSize) {
            if (mutable || writer != this.writer || e != cur) {
                this.writer = writer;
                cur = e;
                while (true) {
                    try {
                        Bytes buffer = this.buffer.obtain(maxSize, true);
                        writer.write(buffer, e);
                        buffer.flip();
                        long size = this.size = buffer.remaining();
                        this.buffer.buffer.position(0L);
                        this.buffer.buffer.limit(size);
                        hash = 0L;
                        return;
                    } catch (Exception ex) {
                        checkMaxSizeStillReasonable(maxSize, ex);
                        maxSize *= 2L;
                    }
                }
            }
        }
    }

    class ForBytesWriter<E, W extends BytesWriter<E>>
            extends CopyingMetaBytesInterop<E, W> {
        private static final long serialVersionUID = 0L;

        private ForBytesWriter() {
            super(DirectBytesBuffer.this);
        }

        void init(W writer, E e, boolean mutable) {
            if (mutable || writer != this.writer || e != cur) {
                this.writer = writer;
                cur = e;
                long size = writer.size(e);
                Bytes buffer = this.buffer.obtain(size, true);
                writer.write(buffer, e);
                buffer.flip();
                this.size = size;
                assert size == buffer.remaining();
                hash = 0L;
            }
        }
    }

    class ForDataValueWriter<E> extends DataValueMetaBytesInterop<E> {

        private ForDataValueWriter() {
            super(DirectBytesBuffer.this);
        }
    }
}
