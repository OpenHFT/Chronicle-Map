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
 * Default value computation strategy, used
 * in {@link ChronicleMapBuilder#defaultValueProvider(DefaultValueProvider)} configuration.
 *
 * @param <K> map key class
 * @param <V> map value class
 */
@FunctionalInterface
public interface DefaultValueProvider<K, V> {

    /**
     * Returns the "nil" value, which should be inserted into the map, in the given
     * {@code absentEntry} context. This is primarily used in {@link ChronicleMap#acquireUsing}
     * operation implementation, i. e. {@link MapMethods#acquireUsing}.
     * <p>
     * The default implementation simply delegates to {@link MapAbsentEntry#defaultValue()}.
     */
    Data<V> defaultValue(@NotNull MapAbsentEntry<K, V> absentEntry);
}
