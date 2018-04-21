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

/**
 * Serializer of objects to bytes, pairing {@link SizedReader}, which knows the length of serialized
 * form of any object before actual serialization 2) doesn't include that length in the serialized
 * form itself, assuming it will be passed by the {@link ChronicleHash} into {@link
 * SizedReader#read} deserialization method.
 * <p>
 * <p>Read <a href="https://github.com/OpenHFT/Chronicle-Map#sizedwriter-and-sizedreader">{@code
 * SizedWriter} and {@code SizedReader}</a> and
 * <a href="https://github.com/OpenHFT/Chronicle-Map#custom-serialization-checklist">custom
 * serialization checklist</a> sections in the Chronicle Map tutorial for more information on this
 * interface, how to implement and use it properly.
 *
 * @param <T> the type of the object marshalled
 * @see SizedReader
 * @see ChronicleHashBuilder#keyMarshallers(SizedReader, SizedWriter)
 * @see ChronicleMapBuilder#valueMarshallers(SizedReader, SizedWriter)
 */
public interface SizedWriter<T> extends Marshallable {

    /**
     * Returns the length (in bytes) of the serialized form of the given object. Serialization form
     * in terms of this interface, i. e. how much bytes are written to {@code out} on
     * {@link #write(Bytes, long, Object) write(out, size, toWrite)} call.
     *
     * @param toWrite the object which serialized form length should be returned
     * @return the length (in bytes) of the serialized form of the given object
     */
    long size(@NotNull T toWrite);

    /**
     * Serializes the given object to the given {@code out}, without writing the length of the
     * serialized form itself.
     * <p>
     * <p>Implementation of this method should increment the {@linkplain Bytes#writePosition()
     * position} of the given {@code out} by {@link #size(Object) size(toWrite)}. The given object
     * should be written into these range between the initial {@code bytes}' position and the
     * position after this method call returns.
     *
     * @param out     the {@code Bytes} to write the given object to
     * @param size    the size, returned by {@link #size(Object)} for the given {@code toWrite} object.
     *                it is given, because size might be needed during serialization, and it's computation has
     *                non-constant complexity, i. e. if serializing a {@code CharSequence} using variable-length
     *                encoding like UTF-8.
     * @param toWrite the object to serialize
     * @see SizedReader#read(Bytes, long, Object)
     */
    void write(Bytes out, long size, @NotNull T toWrite);
}
