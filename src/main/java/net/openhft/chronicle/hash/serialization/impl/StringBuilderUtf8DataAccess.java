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

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.hash.serialization.DataAccess;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.hash.serialization.impl.DefaultElasticBytes.DEFAULT_BYTES_CAPACITY;

public final class StringBuilderUtf8DataAccess
        extends AbstractCharSequenceUtf8DataAccess<StringBuilder> {

    public StringBuilderUtf8DataAccess() {
        this(DEFAULT_BYTES_CAPACITY);
    }

    private StringBuilderUtf8DataAccess(long bytesCapacity) {
        super(bytesCapacity);
    }

    @Override
    public StringBuilder getUsing(@Nullable StringBuilder using) {
        if (using != null) {
            using.setLength(0);
        } else {
            using = new StringBuilder(cs.length());
        }
        using.append(cs);
        return using;
    }

    @Override
    public DataAccess<StringBuilder> copy() {
        return new StringBuilderUtf8DataAccess(bytes().realCapacity());
    }
}
