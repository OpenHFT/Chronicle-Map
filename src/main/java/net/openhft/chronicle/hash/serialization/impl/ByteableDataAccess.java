/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Type;

public class ByteableDataAccess<T extends Byteable> extends InstanceCreatingMarshaller<T>
        implements DataAccess<T>, Data<T> {

    /**
     * State field
     */
    private transient T instance;

    public ByteableDataAccess(Type tClass) {
        super(tClass);
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
        return instance;
    }

    @Override
    public T getUsing(@Nullable T using) {
        if (using == null)
            using = createInstance();
        using.bytesStore(instance.bytesStore(), offset(), size());
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
        return this;
    }

    @Override
    public void uninit() {
        instance = null;
    }

    @Override
    public DataAccess<T> copy() {
        return new ByteableDataAccess<>(tType());
    }
}
