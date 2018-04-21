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
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.values.Copyable;
import net.openhft.chronicle.values.Values;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

public class ValueDataAccess<T> extends AbstractData<T> implements DataAccess<T> {

    /**
     * Config field
     */
    private Class<T> valueType;

    // Cache fields
    private transient Class<? extends T> nativeClass;
    private transient Class<? extends T> heapClass;
    private transient Byteable nativeInstance;
    private transient Copyable nativeInstanceAsCopyable;

    /**
     * State field
     */
    private transient Byteable instance;

    public ValueDataAccess(Class<T> valueType) {
        this.valueType = valueType;
        initTransients();
    }

    /**
     * Returns the interface of values serialized.
     */
    protected Class<T> valueType() {
        return valueType;
    }

    protected Class<? extends T> nativeClass() {
        return nativeClass;
    }

    protected Class<? extends T> heapClass() {
        return heapClass;
    }

    private void initTransients() {
        nativeInstance = (Byteable) Values.newNativeReference(valueType);
        nativeInstanceAsCopyable = (Copyable) nativeInstance;
        nativeClass = (Class<? extends T>) nativeInstance.getClass();
        heapClass = Values.heapClassFor(valueType);
        nativeInstance.bytesStore(allocateBytesStoreForInstance(), 0, nativeInstance.maxSize());
    }

    private BytesStore allocateBytesStoreForInstance() {
        long instanceSize = nativeInstance.maxSize();
        if (instanceSize > Bytes.MAX_BYTE_BUFFER_CAPACITY) {
            return NativeBytesStore.nativeStoreWithFixedCapacity(instanceSize);
        } else {
            return BytesStore.wrap(ByteBuffer.allocate(Maths.toUInt31(instanceSize)));
        }
    }

    protected T createInstance() {
        try {
            return heapClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RandomDataInput bytes() {
        return instance.bytesStore();
    }

    @Override
    public long offset() {
        return instance.offset();
    }

    @Override
    public long size() {
        return instance.maxSize();
    }

    @Override
    public T get() {
        return (T) instance;
    }

    @Override
    public T getUsing(@Nullable T using) {
        if (using == null)
            using = createInstance();
        ((Copyable) using).copyFrom(instance);
        return using;
    }

    @Override
    public Data<T> getData(@NotNull T instance) {
        if (instance.getClass() == nativeClass) {
            this.instance = (Byteable) instance;
        } else {
            nativeInstanceAsCopyable.copyFrom(instance);
            this.instance = nativeInstance;
        }
        return this;
    }

    @Override
    public void uninit() {
        instance = null;
    }

    @Override
    public DataAccess<T> copy() {
        return new ValueDataAccess<>(valueType);
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        valueType = wireIn.read(() -> "valueType").typeLiteral();
        initTransients();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        wireOut.write(() -> "valueType").typeLiteral(valueType);
    }
}
