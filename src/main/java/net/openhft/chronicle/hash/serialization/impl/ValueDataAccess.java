//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.RandomDataInput;
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

@SuppressWarnings({"rawtypes", "unchecked", "deprecation"})
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

    private BytesStore<?, ?> allocateBytesStoreForInstance() {
        long instanceSize = nativeInstance.maxSize();
        if (instanceSize > 0x7FFFFFF0) {
            return BytesStore.nativeStoreWithFixedCapacity(instanceSize);
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
        valueType = wireIn.read("valueType").typeLiteral();
        initTransients();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        wireOut.write("valueType").typeLiteral(valueType);
    }
}
