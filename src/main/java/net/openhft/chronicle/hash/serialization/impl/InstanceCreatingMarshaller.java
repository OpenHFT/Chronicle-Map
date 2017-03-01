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

import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.serialization.DataAccess;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

/**
 * Holds new instance creation logic, common for many {@link DataAccess} and {@link SizedReader}
 * implementations
 *
 * @param <T> the type of objects deserialized
 */
public abstract class InstanceCreatingMarshaller<T> implements Marshallable {

    private Class<T> tClass;

    /**
     * Constructor for use in subclasses.
     *
     * @param tClass the class of objects deserialized
     */
    protected InstanceCreatingMarshaller(Class<T> tClass) {
        this.tClass = tClass;
    }

    /**
     * Returns the class of objects deserialized.
     */
    protected Class<T> tClass() {
        return tClass;
    }

    /**
     * Creates a new {@code T} instance by calling {@link Class#newInstance()}. If you need
     * different logic, i. e. calling a constructor with parameter, override this method in a
     * subclass of the specific {@link DataAccess} or {@link SizedReader} and configure in {@link
     * ChronicleMapBuilder}.
     *
     * @return a new instance to return from {@link Data#getUsing(Object)} or {@link
     * SizedReader#read(net.openhft.chronicle.bytes.Bytes, long, Object)} method
     */
    protected T createInstance() {
        try {
            return ObjectUtils.newInstance(tClass);
        } catch (Exception e) {
            throw new IllegalStateException("Some of default marshallers, chosen for the type\n" +
                    tClass + " by default, delegate to \n" +
                    this.getClass().getName() + " which assumes the type has a public no-arg\n" +
                    "constructor. If this is not true, you should either extend the marshaller,\n" +
                    "overriding createInstance() and copy() (if defined), and the extending\n" +
                    "class shouldn't be inner, because such classes couldn't be Serializable\n" +
                    "that is a requirement for marshaller classes, or write and configure your\n" +
                    "own marshaller for " + tClass + " type from scratch, and configure for the\n" +
                    "Chronicle Map via keyMarshaller[s]() or valueMarshaller[s]() methods", e);
        }
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        //noinspection unchecked
        tClass = wireIn.read(() -> "tClass").typeLiteral();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        wireOut.write(() -> "tClass").typeLiteral(tClass);
    }
}
