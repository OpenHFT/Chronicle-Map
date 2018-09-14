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

public final class StringUtf8DataAccess extends AbstractCharSequenceUtf8DataAccess<String> {

    public StringUtf8DataAccess() {
        this(DefaultElasticBytes.DEFAULT_BYTES_CAPACITY);
    }

    private StringUtf8DataAccess(long bytesCapacity) {
        super(bytesCapacity);
    }

    @Override
    public String getUsing(@Nullable String using) {
        return cs;
    }

    @Override
    public DataAccess<String> copy() {
        return new StringUtf8DataAccess(bytes().realCapacity());
    }
}
