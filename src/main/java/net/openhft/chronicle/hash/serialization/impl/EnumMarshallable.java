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
