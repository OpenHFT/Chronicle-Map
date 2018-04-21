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

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.StatefulCopyable;
import net.openhft.chronicle.values.Copyable;
import net.openhft.chronicle.values.Values;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ValueReader<T>
        implements SizedReader<T>, BytesReader<T>, StatefulCopyable<ValueReader<T>> {

    /**
     * Config field
     */
    private Class<T> valueType;

    // Cache fields
    private transient Class<? extends T> nativeClass;
    private transient Class<? extends T> heapClass;
    private transient Byteable nativeReference;

    public ValueReader(Class<T> valueType) {
        this.valueType = valueType;
        initTransients();
    }

    /**
     * Returns the interface of values deserialized.
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
        nativeClass = Values.nativeClassFor(valueType);
        heapClass = Values.heapClassFor(valueType);
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
