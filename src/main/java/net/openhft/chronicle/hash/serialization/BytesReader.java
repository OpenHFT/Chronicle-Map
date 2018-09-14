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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.ChronicleHashBuilder;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.wire.Marshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Deserializer of objects from bytes, pairing {@link BytesWriter}, without accepting additional
 * information about serialized objects.
 * <p>
 * <p>Read <a href="https://github.com/OpenHFT/Chronicle-Map#byteswriter-and-bytesreader">{@code
 * BytesWriter} and {@code BytesReader}</a> and
 * <a href="https://github.com/OpenHFT/Chronicle-Map#custom-serialization-checklist">custom
 * serialization checklist</a> sections in the Chronicle Map tutorial for more information on this
 * interface, how to implement and use it properly.
 *
 * @param <T> type of objects deserialized
 * @see BytesWriter
 * @see ChronicleHashBuilder#keyMarshallers(BytesReader, BytesWriter)
 * @see ChronicleMapBuilder#valueMarshallers(BytesReader, BytesWriter)
 */
public interface BytesReader<T> extends Marshallable {

    /**
     * Reads and returns the object from {@link Bytes#readPosition()} (i. e. the current position)
     * in the given {@code in}. Should attempt to reuse the given {@code using} object, i. e. to
     * read the deserialized data into the given object. If it is possible, this object then
     * returned from this method back. If it is impossible for any reason, a new object should be
     * created and returned. The given {@code using} object could be {@code null}, in this case this
     * method, of cause, should create a new object.
     * <p>
     * <p>This method should increment the position in the given {@code Bytes}, i. e. consume the
     * read bytes. {@code in} bytes shouldn't be written.
     *
     * @param in    the {@code Bytes} to read the object from
     * @param using the object to read the deserialized data into, could be {@code null}
     * @return the object read from the bytes, either reused or newly created
     */
    @NotNull
    T read(Bytes in, @Nullable T using);
}
