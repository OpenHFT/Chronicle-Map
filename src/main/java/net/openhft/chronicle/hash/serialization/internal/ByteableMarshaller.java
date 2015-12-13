/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.hash.serialization.internal;

import net.openhft.chronicle.hash.hashing.Hasher;
import net.openhft.chronicle.hash.serialization.BytesInterop;
import net.openhft.chronicle.hash.serialization.DeserializationFactoryConfigurableBytesReader;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.MultiStoreBytes;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.io.serialization.ObjectFactory;
import net.openhft.lang.model.Byteable;
import org.jetbrains.annotations.NotNull;

public abstract class ByteableMarshaller<E extends Byteable>
        implements BytesInterop<E>, MetaBytesInterop<E, Object>,
        DeserializationFactoryConfigurableBytesReader<E, ByteableMarshaller<E>>,
        SizeMarshaller {
    private static final long serialVersionUID = 0L;

    public static <E extends Byteable> ByteableMarshaller<E> of(@NotNull Class<E> eClass) {
        return new Default<>(eClass);
    }

    @Override
    public ByteableMarshaller<E> withDeserializationFactory(
            ObjectFactory<E> deserializationFactory) {
        return new WithCustomFactory<>(tClass, deserializationFactory);
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
    public long minEncodableSize() {
        return size;
    }

    @Override
    public int minSizeEncodingSize() {
        return 0;
    }

    @Override
    public int maxSizeEncodingSize() {
        return 0;
    }

    @Override
    public void writeSize(Bytes bytes, long size) {
        // do nothing
    }

    @Override
    public boolean startsWith(Bytes bytes, E e) {
        if (bytes.capacity() - bytes.position() < size)
            return false;
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
        Bytes eBytes = e.bytes();
        if (eBytes != null) {
            bytes.write(eBytes, e.offset(), size);
        } else {
            throw new NullPointerException("You are trying to write a byteable object of " +
                    e.getClass() + ", " +
                    "which bytes are not assigned. I. e. most likely the object is uninitialized.");
        }
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
    public E read(Bytes bytes, long size, E toReuse) {
        try {
            if (toReuse == null)
                toReuse = getInstance();
            setBytesAndOffset(toReuse, bytes);
            bytes.skip(size);
            return toReuse;
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

    @Override
    public boolean startsWith(Object interop, Bytes bytes, E e) {
        return startsWith(bytes, e);
    }

    @Override
    public long hash(Object interop, E e) {
        return hash(e);
    }

    @Override
    public long size(Object writer, E e) {
        return size(e);
    }

    @Override
    public void write(Object writer, Bytes bytes, E e) {
        write(bytes, e);
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
