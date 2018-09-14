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

package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.HashEntry;

/**
 * A context of a <i>present</i> entry in the {@link ChronicleSet}.
 *
 * @param <K> the set key type
 * @see SetEntryOperations
 * @see SetQueryContext#entry()
 */
public interface SetEntry<K> extends HashEntry<K> {
    @Override
    SetContext<K, ?> context();

    /**
     * Removes the entry from the {@code ChronicleSet}.
     * <p>
     * <p>This method is the default implementation for {@link SetEntryOperations#remove(SetEntry)},
     * which might be customized over the default.
     */
    @Override
    void doRemove();
}
