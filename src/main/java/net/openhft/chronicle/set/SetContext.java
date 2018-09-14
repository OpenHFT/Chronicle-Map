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

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.HashContext;

/**
 * Context, in which {@link SetEntry SetEntries} are accessed. {@code SetContext} allows to access
 * {@link SetEntryOperations}, configured for the accessed {@link ChronicleSet}. {@code SetContext}
 * implements {@code SetEntryOperations} by delegation to the configured {@code entryOperations}.
 *
 * @param <K> the set key type
 * @param <R> the return type of {@link SetEntryOperations} specified for the queried set
 */
public interface SetContext<K, R> extends HashContext<K>, SetEntryOperations<K, R> {

    /**
     * Returns the accessed {@code ChronicleSet}. Synonym to {@link #set()}.
     */
    @Override
    ChronicleHash<K, ?, ?, ?> hash();

    /**
     * Returns the accessed {@code ChronicleSet}. Synonym to {@link #hash()}.
     */
    ChronicleSet<K> set();
}
