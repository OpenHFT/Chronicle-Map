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

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.hash.serialization.SizedReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class StringBuilderSizedReader
        implements SizedReader<StringBuilder>, EnumMarshallable<StringBuilderSizedReader> {
    public static final StringBuilderSizedReader INSTANCE = new StringBuilderSizedReader();

    private StringBuilderSizedReader() {
    }

    @NotNull
    @Override
    public StringBuilder read(Bytes in, long size, @Nullable StringBuilder using) {
        if (0 > size || size > Integer.MAX_VALUE)
            throw new IllegalStateException("positive int size expected, " + size + " given");
        int csLen = (int) size;
        if (using == null) {
            using = new StringBuilder(csLen);
        } else {
            using.setLength(0);
            using.ensureCapacity(csLen);
        }
        BytesUtil.parseUtf8(in, using, csLen);
        return using;
    }

    @Override
    public StringBuilderSizedReader readResolve() {
        return INSTANCE;
    }
}
