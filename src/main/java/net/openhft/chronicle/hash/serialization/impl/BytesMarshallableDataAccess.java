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
import net.openhft.chronicle.bytes.BytesMarshallable;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;

public class BytesMarshallableDataAccess<T extends BytesMarshallable>
        extends InstanceCreatingMarshaller<T> implements DataAccess<T>, Data<T> {

    /** Cache field */
    private transient Bytes bytes;

    /** State field */
    private transient T instance;

    public BytesMarshallableDataAccess(Class<T> tClass) {
        super(tClass);
        initTransients();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
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
        bytes.clear();
        instance.writeMarshallable(bytes);
        return this;
    }

    @Override
    public void uninit() {
        instance = null;
    }

    @Override
    public DataAccess<T> copy() {
        return new BytesMarshallableDataAccess<>(tClass);
    }
}
