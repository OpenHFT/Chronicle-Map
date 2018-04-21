/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
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

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.wire.WireIn;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.hash.serialization.impl.DefaultElasticBytes.DEFAULT_BYTES_CAPACITY;

public class BytesMarshallableDataAccess<T extends BytesMarshallable>
        extends InstanceCreatingMarshaller<T> implements DataAccess<T>, Data<T> {

    // Cache fields
    private transient boolean bytesInit;
    private transient Bytes bytes;
    private transient VanillaBytes targetBytes;

    /**
     * State field
     */
    private transient T instance;

    public BytesMarshallableDataAccess(Class<T> tClass) {
        this(tClass, DEFAULT_BYTES_CAPACITY);
    }

    private BytesMarshallableDataAccess(Class<T> tClass, long bytesCapacity) {
        super(tClass);
        initTransients(bytesCapacity);
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        super.readMarshallable(wireIn);
        initTransients(DEFAULT_BYTES_CAPACITY);
    }

    private void initTransients(long bytesCapacity) {
        bytes = DefaultElasticBytes.allocateDefaultElasticBytes(bytesCapacity);
        targetBytes = VanillaBytes.vanillaBytes();
    }

    @Override
    public RandomDataInput bytes() {
        initBytes();
        return bytes.bytesStore();
    }

    private void initBytes() {
        if (!bytesInit) {
            bytes.clear();
            instance.writeMarshallable(bytes);
            bytesInit = true;
        }
    }

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public long size() {
        initBytes();
        return bytes.readRemaining();
    }

    @Override
    public void writeTo(RandomDataOutput target, long targetOffset) {
        if (bytesInit) {
            target.write(targetOffset, bytes(), offset(), size());
        } else {
            targetBytes.bytesStore((BytesStore) target, targetOffset,
                    target.capacity() - targetOffset);
            targetBytes.writePosition(targetOffset);
            instance.writeMarshallable(targetBytes);
            targetBytes.bytesStore(NoBytesStore.NO_BYTES_STORE, 0, 0);
        }
    }

    @Override
    public T get() {
        return instance;
    }

    @Override
    public T getUsing(@Nullable T using) {
        if (using == null)
            using = createInstance();
        initBytes();
        using.readMarshallable(bytes);
        bytes.readPosition(0);
        return using;
    }

    @Override
    public int hashCode() {
        return dataHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return dataEquals(obj);
    }

    @Override
    public String toString() {
        return get().toString();
    }

    @Override
    public Data<T> getData(@NotNull T instance) {
        this.instance = instance;
        bytesInit = false;
        return this;
    }

    @Override
    public void uninit() {
        instance = null;
    }

    @Override
    public DataAccess<T> copy() {
        return new BytesMarshallableDataAccess<>(tClass(), bytes.realCapacity());
    }
}
