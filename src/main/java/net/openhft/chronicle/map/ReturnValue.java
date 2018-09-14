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
