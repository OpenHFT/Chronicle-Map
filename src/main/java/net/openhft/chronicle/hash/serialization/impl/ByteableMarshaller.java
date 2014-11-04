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

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.hash.serialization.AgileBytesMarshaller;
import net.openhft.chronicle.hash.serialization.Hasher;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.MultiStoreBytes;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.io.serialization.*;
import net.openhft.lang.io.serialization.impl.AllocateInstanceObjectFactory;
import net.openhft.lang.model.Byteable;
import org.jetbrains.annotations.NotNull;

public class ByteableMarshaller<E extends Byteable> implements AgileBytesMarshaller<E> {
    private static final long serialVersionUID = 0L;

    public static <E extends Byteable> ByteableMarshaller<E> of(@NotNull Class<E> eClass) {
        return new ByteableMarshaller<E>(eClass);
    }

    public static <E extends Byteable> ByteableMarshaller<E> of(@NotNull Class<E> eClass,
                                                                ObjectFactory<E> factory) {
        if (factory instanceof AllocateInstanceObjectFactory) {
            Class allocatedClass = ((AllocateInstanceObjectFactory) factory).allocatedClass();
            return new Default<E>(allocatedClass);
        } else {
            return new WithCustomFactory<E>(eClass, factory);
        }
    }

    @NotNull
    private final Class<E> tClass;
    private long size;

    private ByteableMarshaller(@NotNull Class<E> tClass) {
        this.tClass = tClass;
    }

    void initSize() {
        try {
            size = getInstance().maxSize();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public long size(E e) {
        return size;
    }

    @Override
    public int sizeEncodingSize(long size) {
        return 0;
    }

    @Override
    public void writeSize(Bytes bytes, long size) {
        // do nothing
    }

    @Override
    public boolean startsWith(Bytes bytes, E e) {
        Bytes input = e.bytes();
        long pos = bytes.position(), inputPos = e.offset();

        int i = 0;
        for (; i < size - 7; i += 8) {
            if (bytes.readLong(pos + i) != input.readLong(inputPos + i))
                return false;
        }
        for (; i < size; i++) {
            if (bytes.readByte(pos + i) != input.readByte(inputPos + i))
                return false;
        }
        return true;
    }

    @Override
    public long hash(E e) {
        return Hasher.hash(e.bytes(), e.offset(), e.offset() + size);
    }

    @Override
    public void write(Bytes bytes, E e) {
        bytes.write(e.bytes(), e.offset(), size);
    }

    @Override
    public long readSize(Bytes bytes) {
        return size;
    }

    @Override
    public E read(Bytes bytes, long size) {
        return read(bytes, size, null);
    }

    @Override
    public E read(Bytes bytes, long size, E e) {
        try {
            if (e == null)
                e = getInstance();
            setBytesAndOffset(e, bytes);
            bytes.skip(size);
            return e;
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    public static void setBytesAndOffset(Byteable e, Bytes bytes) {
        // use the unshifting underlying Bytes (the position never changes)
        // if you use a Bytes where the position changes, you risk concurrency issues.
        if (bytes instanceof MultiStoreBytes) {
            MultiStoreBytes msb = (MultiStoreBytes) bytes;
            e.bytes(msb.underlyingBytes(), msb.underlyingOffset() + msb.position());
        } else {
            e.bytes(bytes, bytes.position());
        }
    }

    @SuppressWarnings("unchecked")
    @NotNull
    E getInstance() throws Exception {
        return (E) NativeBytes.UNSAFE.allocateInstance(tClass);
    }

    private static class Default<E extends Byteable> extends ByteableMarshaller<E> {
        private static final long serialVersionUID = 0L;

        Default(@NotNull Class<E> tClass) {
            super(tClass);
            initSize();
        }
    }

    private static class WithCustomFactory<E extends Byteable> extends ByteableMarshaller<E> {
        private static final long serialVersionUID = 0L;

        @NotNull
        private final ObjectFactory<E> factory;

        WithCustomFactory(@NotNull Class<E> tClass,
                                            @NotNull ObjectFactory<E> factory) {
            super(tClass);
            this.factory = factory;
            initSize();
        }

        @NotNull
        @Override
        E getInstance() throws Exception {
            return factory.create();
        }
    }
}
