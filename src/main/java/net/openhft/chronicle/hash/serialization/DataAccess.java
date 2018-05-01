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

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.ChronicleHashBuilder;
import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.wire.Marshallable;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * Strategy of accessing flat byte contents of objects, using a reusable {@link Data} object, cached
 * inside the {@code DataAccess} instance. This strategy interface is suitable for types of objects,
 * that are already in fact sequences of bytes ({@code byte[]}, {@link ByteBuffer}, etc.) and
 * shouldn't be serialized, and allows to avoid extra data copying. Accessed bytes should be later
 * readable by some {@link BytesReader}.
 * <p>
 * <p>Read <a href="https://github.com/OpenHFT/Chronicle-Map#dataaccess-and-sizedreader">{@link
 * DataAccess} and {@code SizedReader}</a> and
 * <a href="https://github.com/OpenHFT/Chronicle-Map#custom-serialization-checklist">custom
 * serialization checklist</a> sections in the Chronicle Map tutorial for more information on this
 * interface, how to implement and use it properly.
 *
 * @param <T> the type of objects accessed
 * @see ChronicleHashBuilder#keyReaderAndDataAccess(SizedReader, DataAccess)
 * @see ChronicleMapBuilder#valueReaderAndDataAccess(SizedReader, DataAccess)
 */
public interface DataAccess<T> extends StatefulCopyable<DataAccess<T>>, Marshallable {

    /**
     * Obtain {@code Data} accessor to the bytes of the given object. Typically a {@code Data}
     * instance should be cached in a field of this {@code DataAccess}, so always the same object
     * is returned.
     *
     * @param instance the object to access bytes (serialized form) of
     * @return an accessor to the bytes of the given object
     */
    Data<T> getData(@NotNull T instance);

    /**
     * Clear references to the accessed object (provided in {@link #getData(Object)} method) in
     * caching fields of this {@code DataAccess} or the cached {@link Data} object, returned from
     * {@link #getData(Object)}.
     * <p>
     * {@code DataAccess} is cached itself in thread-local variables for {@link ChronicleHash}
     * instances, this method prevents leaking of accessed objects (they are not eligible for
     * garbage collection while there are some references).
     */
    void uninit();
}
