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

import net.openhft.chronicle.hash.HashQueryContext;
import org.jetbrains.annotations.Nullable;

/**
 * A context of {@link ChronicleSet} operations with <i>individual keys</i>
 * (most: {@code contains()}, {@code add()}, etc., opposed to <i>bulk</i> operations).
 *
 * @param <K> the set key type
 * @param <R> the return type of {@link SetEntryOperations} specialized for the queried set
 * @see ChronicleSet#queryContext(Object)
 */
public interface SetQueryContext<K, R> extends HashQueryContext<K>, SetContext<K, R> {

    @Override
    @Nullable
    SetEntry<K> entry();

    @Override
    @Nullable
    SetAbsentEntry<K> absentEntry();
}
