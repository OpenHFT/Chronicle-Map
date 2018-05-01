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

package net.openhft.chronicle.hash.serialization;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.ChronicleHashBuilder;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.wire.Marshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Deserializer of objects from bytes, pairing {@link SizedWriter}, i. e. assuming the length
 * of the serialized form isn't written in the beginning of the serialized form itself, but managed
 * by {@link ChronicleHash} implementation and passed to the reading methods.
 * <p>
 * <p>Read <a href="https://github.com/OpenHFT/Chronicle-Map#sizedwriter-and-sizedreader">{@code
 * SizedWriter} and {@code SizedReader}</a>,
 * <a href="https://github.com/OpenHFT/Chronicle-Map#dataaccess-and-sizedreader">{@link DataAccess}
 * and {@code SizedReader}</a> and
 * <a href="https://github.com/OpenHFT/Chronicle-Map#custom-serialization-checklist">custom
 * serialization checklist</a> sections in the Chronicle Map tutorial for more information on this
 * interface, how to implement and use it properly.
 *
 * @param <T> the type of the object deserialized
 * @see SizedWriter
 * @see ChronicleHashBuilder#keyMarshallers(SizedReader, SizedWriter)
 * @see ChronicleHashBuilder#keyReaderAndDataAccess(SizedReader, DataAccess)
 * @see ChronicleMapBuilder#valueMarshallers(SizedReader, SizedWriter)
 * @see ChronicleMapBuilder#valueReaderAndDataAccess(SizedReader, DataAccess)
 */
public interface SizedReader<T> extends Marshallable {

    /**
     * Reads and returns the object from {@link Bytes#readPosition()} (i. e. the current position)
     * to {@code Bytes.readPosition() + size} in the given {@code in}. Should attempt to reuse the
     * given {@code using} object, i. e. to read the deserialized data into the given object. If it
     * is possible, this objects then returned from this method. If it is impossible for any reason,
     * a new object should be created and returned. The given {@code using} object could be {@code
     * null}, in this case read() should always create a new object.
     * <p>
     * <p>This method should increment the position in the given {@code Bytes} by the given {@code
     * size}.
     *
     * @param in    the {@code Bytes} to read the object from
     * @param size  the size of the serialized form of the returned object
     * @param using the object to read the deserialized data into, could be {@code null}
     * @return the object read from the bytes, either reused or newly created
     */
    @NotNull
    T read(Bytes in, long size, @Nullable T using);
}
