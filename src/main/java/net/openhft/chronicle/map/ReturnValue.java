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

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.Data;
import org.jetbrains.annotations.NotNull;

/**
 * Abstracts returning a value from a query to a {@link ChronicleMap}, used in {@link MapMethods}.
 * This interface is not supposed to be implemented in user code.
 *
 * @param <V> the type of values in the {@code ChronicleMap}
 */
public interface ReturnValue<V> {
    /**
     * Calling this method on a {@code ReturnValue} object, provided as an argument in a method from
     * {@link MapMethods}, designates that the {@code ChronicleMap}'s method (backed by this
     * {@code MapMethods}'s method) should return the given {@code value}.
     * <p>
     * <p>It is not allowed to call {@code returnValue()} twice during a single {@code MapMethods}'s
     * method call.
     * <p>
     * <p>Not calling {@code returnValue()} during {@code MapMethods}'s method call will make the
     * backed {@code ChronicleMap}'s method to return {@code null}.
     *
     * @param value a {@code Data} object wrapping a value, which should be returned from a
     *              {@code ChronicleMap}'s method, backed by the {@code MapMethods}'s method, to
     *              which this {@code ReturnValue} instance was passed as an argument
     */
    void returnValue(@NotNull Data<V> value);
}
