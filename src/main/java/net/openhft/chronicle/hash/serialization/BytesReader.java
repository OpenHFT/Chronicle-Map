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

package net.openhft.chronicle.hash.serialization;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.wire.Marshallable;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

/**
 * External version on {@link ReadBytesMarshallable}.
 *
 * @param <T> type of objects deserialized
 * @see BytesWriter
 */
public interface BytesReader<T> extends Serializable, Marshallable {

    /**
     * Reads and returns the object from {@link Bytes#readPosition()} (i. e. the current position)
     * in the given {@code in}. Should attempt to reuse the given {@code using} object, i. e. to
     * read the deserialized data into the given object. If it is possible, this object then
     * returned from this method back. If it is impossible for any reason, a new object should be
     * created and returned. The given {@code using} object could be {@code null}, in this case this
     * method, of cause, should create a new object.
     *
     * <p>This method should increment the position in the given {@code Bytes}, i. e. consume the
     * read bytes. {@code in} bytes shouldn't be written.
     *
     * @param in the {@code Bytes} to read the object from
     * @param using the object to read the deserialized data into, could be {@code null}
     * @return the object read from the bytes, either reused or newly created
     */
    @NotNull
    T read(Bytes in, T using);
}
