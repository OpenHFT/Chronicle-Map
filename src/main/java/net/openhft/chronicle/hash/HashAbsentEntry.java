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

package net.openhft.chronicle.hash;

import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.set.SetAbsentEntry;
import org.jetbrains.annotations.NotNull;

/**
 * Low-level operational context for the situations, when the new entry is going to be inserted
 * into the {@link ChronicleHash}.
 * <p>
 * <p>This interface is not usable by itself; it merely defines the common base for {@link
 * MapAbsentEntry} and {@link SetAbsentEntry}.
 *
 * @param <K> the hash key type
 * @see HashQueryContext#absentEntry()
 */
public interface HashAbsentEntry<K> {

    /**
     * Returns the context, in which the entry is going to be inserted into the hash.
     */
    HashContext<K> context();

    /**
     * Returns the key is going to be inserted into the {@code ChronicleHash}.
     */
    @NotNull
    Data<K> absentKey();
}
