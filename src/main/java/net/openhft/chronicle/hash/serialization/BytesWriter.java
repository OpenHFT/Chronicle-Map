//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.chronicle.hash.serialization;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.ChronicleHashBuilder;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.wire.Marshallable;
import org.jetbrains.annotations.NotNull;

/**
 * Serializer of objects to bytes, pairing {@link BytesReader}.
 * <p>
 * Read <a href="https://github.com/OpenHFT/Chronicle-Map/blob/ea/docs/CM_Tutorial_Bytes.adoc">{@code
 * BytesWriter} and {@code BytesReader}</a> and
 * <a href="https://github.com/OpenHFT/Chronicle-Map/blob/ea/docs/CM_Tutorial_Bytes.adoc">custom
 * serialization checklist</a> sections in the Chronicle Map tutorial for more information on this
 * interface, how to implement and use it properly.
 *
 * @param <T> the type of objects serialized
 * @see BytesReader
 * @see ChronicleHashBuilder#keyMarshallers(BytesReader, BytesWriter)
 * @see ChronicleMapBuilder#valueMarshallers(BytesReader, BytesWriter)
 */
public interface BytesWriter<T> extends Marshallable {

    /**
     * Serializes the given object to the given {@code out}.
     * <p>
     * Implementation of this method should increment the {@linkplain Bytes#writePosition()
     * position} of the given {@code out} by the number of bytes written. The given object should be
     * written into these range between the initial {@code bytes}' position and the position after
     * this method call returns. Bytes outside of this range shouldn't be written. Any bytes
     * shouldn't be read from {@code out}.
     *
     * @param out     the {@code Bytes} to write the given object to
     * @param toWrite the object to serialize
     */
    void write(Bytes<?> out, @NotNull T toWrite);
}
