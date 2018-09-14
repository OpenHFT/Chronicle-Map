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

import net.openhft.chronicle.map.MapEntry;
import net.openhft.chronicle.set.SetEntry;
import org.jetbrains.annotations.NotNull;

/**
 * A context of a <i>present</i> entry in the {@code ChronicleHash}.
 * <p>
 * <p>This interface is not usable by itself; it merely defines the common base for {@link MapEntry}
 * and {@link SetEntry}.
 *
 * @param <K> type of the key in {@code ChronicleHash}
 * @see HashQueryContext#entry()
 */
public interface HashEntry<K> {
    /**
     * Returns the context, in which the entry is accessed.
     */
    HashContext<K> context();

    /**
     * Returns the entry key.
     */
    @NotNull
    Data<K> key();

    /**
     * Removes the entry from the {@code ChronicleHash}.
     *
     * @throws IllegalStateException if some locking/state conditions required to perform remove
     *                               operation are not met
     */
    void doRemove();
}
