/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
