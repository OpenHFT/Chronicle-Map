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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded;

public class ExternalBytesMarshallableDataAccess<T> extends InstanceCreatingMarshaller<T>
        implements DataAccess<T>, Data<T> {

    // Config fields
    private SizedReader<T> reader;
    private BytesWriter<? super T> writer;

    /** Cache field */
    private transient Bytes bytes;

    /** State field */
    private transient T instance;

    public ExternalBytesMarshallableDataAccess(
            Class<T> tClass, SizedReader<T> reader, BytesWriter<? super T> writer) {
        super(tClass);
        this.writer = writer;
        this.reader = reader;
        initTransients();
    }

    private void initTransients() {
        bytes = Bytes.allocateElasticDirect(1);
    }

    @Override
    public RandomDataInput bytes() {
        return bytes.bytesStore();
    }

    @Override
    public long offset() {
        return 0;
    }

    @Override
    public long size() {
        return bytes.readRemaining();
    }

    @Override
    public T get() {
        return instance;
    }

    @Override
    public T getUsing(@Nullable T using) {
        if (using == null)
            using = createInstance();
        T result = reader.read(bytes, size(), using);
        bytes.readPosition(0);
        return result;
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
        bytes.clear();
        writer.write(bytes, instance);
        return this;
    }

    @Override
    public void uninit() {
        instance = null;
    }

    @Override
    public DataAccess<T> copy() {
        return new ExternalBytesMarshallableDataAccess<>(
                tClass(), copyIfNeeded(reader), copyIfNeeded(writer));
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        super.readMarshallable(wireIn);
        reader = wireIn.read(() -> "reader").typedMarshallable();
        writer = wireIn.read(() -> "writer").typedMarshallable();
        initTransients();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        super.writeMarshallable(wireOut);
        wireOut.write(() -> "reader").typedMarshallable(reader);
        wireOut.write(() -> "writer").typedMarshallable(writer);
    }
}
