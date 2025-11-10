//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

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
 * Read <a href="https://github.com/OpenHFT/Chronicle-Map/blob/ea/docs/CM_Tutorial_DataAccess.adoc">{@code
 * SizedWriter} and {@code SizedReader}</a>,
 * <a href="https://github.com/OpenHFT/Chronicle-Map/blob/ea/docs/CM_Tutorial_DataAccess.adoc">{@link DataAccess}
 * and {@code SizedReader}</a> and
 * <a href="https://github.com/OpenHFT/Chronicle-Map/blob/ea/docs/CM_Tutorial.adoc#custom-serialization-checklist">custom
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
     * This method should increment the position in the given {@code Bytes} by the given {@code
     * size}.
     *
     * @param in    the {@code Bytes} to read the object from
     * @param size  the size of the serialized form of the returned object
     * @param using the object to read the deserialized data into, could be {@code null}
     * @return the object read from the bytes, either reused or newly created
     */
    @NotNull
    T read(Bytes<?> in, long size, @Nullable T using);
}
