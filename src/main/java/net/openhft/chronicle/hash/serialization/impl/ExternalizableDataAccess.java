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

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static net.openhft.chronicle.hash.serialization.impl.DefaultElasticBytes.DEFAULT_BYTES_CAPACITY;

public class ExternalizableDataAccess<T extends Externalizable> extends SerializableDataAccess<T> {

    /**
     * Config field
     */
    private Class<T> tClass;

    public ExternalizableDataAccess(Class<T> tClass) {
        this(tClass, DEFAULT_BYTES_CAPACITY);
    }

    private ExternalizableDataAccess(Class<T> tClass, long bytesCapacity) {
        super(bytesCapacity);
        this.tClass = tClass;
    }

    protected Class<T> tClass() {
        return tClass;
    }

    protected T createInstance() {
        try {
            return tClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(
                    "Externalizable " + tClass + " must have a public no-arg constructor", e);
        }
    }

    @Override
    public T getUsing(@Nullable T using) {
        if (using == null)
            using = createInstance();
        try {
            using.readExternal(new ObjectInputStream(in));
            bytes.readPosition(0);
            return using;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Data<T> getData(@NotNull T instance) {
        this.instance = instance;
        bytes.clear();
        try {
            ObjectOutputStream out = new ObjectOutputStream(this.out);
            instance.writeExternal(out);
            out.flush();
            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataAccess<T> copy() {
        return new ExternalizableDataAccess<>(tClass, bytes.realCapacity());
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        tClass = wireIn.read(() -> "tClass").typeLiteral();
        initTransients(DEFAULT_BYTES_CAPACITY);
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        wireOut.write(() -> "tClass").typeLiteral(tClass);
    }
}
