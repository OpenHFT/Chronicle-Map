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

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.AbstractData;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.values.Copyable;
import net.openhft.chronicle.values.Values;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;

import static net.openhft.chronicle.bytes.NativeBytesStore.nativeStoreWithFixedCapacity;

public class ValueDataAccess<T> extends AbstractData<T> implements DataAccess<T> {

    /** The interface of values serialized. */
    protected final Class<T> valueType;

    // Cache fields
    protected transient Class<? extends T> nativeClass;
    protected transient Class<? extends T> heapClass;
    private transient Byteable nativeInstance;
    private transient Copyable nativeInstanceAsCopyable;

    /** State field */
    private transient Byteable instance;

    public ValueDataAccess(Class<T> valueType) {
        this.valueType = valueType;
        initTransients();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        initTransients();
    }

    private void initTransients() {
        nativeInstance = (Byteable) Values.newNativeReference(valueType);
        nativeInstanceAsCopyable = (Copyable) nativeInstance;
        nativeClass = (Class<? extends T>) nativeInstance.getClass();
        heapClass = Values.heapClassFor(valueType);
        nativeInstance.bytesStore(nativeStoreWithFixedCapacity(nativeInstance.maxSize()),
                0, nativeInstance.maxSize());
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
}
