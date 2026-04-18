/*
 * Copyright 2013-2026 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.core.util.ReadResolvable;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

/**
 * This interface safes many single-constant enum-like subclasses in the
 * {@link net.openhft.chronicle.hash.serialization.impl} package from implementing empty
 * {@link #readMarshallable} and {@link #writeMarshallable} methods, and forces them to implement
 * {@link ReadResolvable}. The name references "Enum" because formerly it allowed only {@code enum}
 * subclasses, to enforce a good practice of making enum-like classes actually Java {@code enum}s.
 * But changes in Chronicle-Wire, used to serialize/deserialize marshallers in Chronicle Map
 * headers, turned {@code enum}s from recommended form of marshaller implementations to impossible
 * one. So now there are no subclasses of this interface which are actually {@code enum}s.
 *
 * @param <E> the subclassing marshaller type
 */
public interface EnumMarshallable<E> extends Marshallable, ReadResolvable<E> {

    @Override
    default void readMarshallable(@NotNull WireIn wireIn) {
        // shouldn't read fields, anyway readResolved later
    }

    @Override
    default void writeMarshallable(@NotNull WireOut wireOut) {
        // shouldn't read => shouldn't write fields
    }
}
