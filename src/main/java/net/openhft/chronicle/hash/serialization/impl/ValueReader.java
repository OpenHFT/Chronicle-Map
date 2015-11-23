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
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.StatefulCopyable;
import net.openhft.chronicle.values.Copyable;
import net.openhft.chronicle.values.Values;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;

public class ValueReader<T>
        implements SizedReader<T>, BytesReader<T>, StatefulCopyable<ValueReader<T>> {

    /** The interface of values deserialized. */
    protected final Class<T> valueType;

    // Cache fields
    protected transient Class<? extends T> heapClass;
    protected transient Class<? extends T> nativeClass;
    private transient Byteable nativeReference;

    public ValueReader(Class<T> valueType) {
        this.valueType = valueType;
        initTransients();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        initTransients();
    }

    private void initTransients() {
        heapClass = Values.heapClassFor(valueType);
        nativeClass = Values.nativeClassFor(valueType);
        nativeReference = (Byteable) Values.newNativeReference(valueType);
    }

    protected T createInstance() {
        try {
            return heapClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    @Override
    public T read(@NotNull Bytes in, long size, @Nullable T using) {
        if (size != nativeReference.maxSize())
            throw new IllegalArgumentException();
        return read(in, using);
    }

    @NotNull
    @Override
    public T read(Bytes in, @Nullable T using) {
        if (using != null && using.getClass() == nativeClass) {
            ((Byteable) using).bytesStore(in.bytesStore(), in.readPosition(),
                    nativeReference.maxSize());
            return using;
        }
        if (using == null)
            using = createInstance();
        nativeReference.bytesStore(in.bytesStore(), in.readPosition(),
                nativeReference.maxSize());
        ((Copyable) using).copyFrom(nativeReference);
        return using;
    }

    @Override
    public ValueReader<T> copy() {
        return new ValueReader<>(valueType);
    }
}
