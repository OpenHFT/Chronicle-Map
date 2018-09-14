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
import net.openhft.chronicle.core.util.ReadResolvable;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.StatefulCopyable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CharSequenceSizedReader implements SizedReader<CharSequence>,
        StatefulCopyable<CharSequenceSizedReader>, ReadResolvable<CharSequenceSizedReader> {

    public static final CharSequenceSizedReader INSTANCE = new CharSequenceSizedReader();

    /**
     * @deprecated use {@link #INSTANCE} as {@code CharSequenceSizedReader} is immutable and
     * stateless
     */
    @Deprecated
    public CharSequenceSizedReader() {
    }

    @NotNull
    @Override
    public CharSequence read(
            @NotNull Bytes in, long size, @Nullable CharSequence using) {
        if (0 > size || size > Integer.MAX_VALUE)
            throw new IllegalStateException("positive int size expected, " + size + " given");
        int csLen = (int) size;
        StringBuilder usingSB;
        if (using instanceof StringBuilder) {
            usingSB = ((StringBuilder) using);
            usingSB.setLength(0);
            usingSB.ensureCapacity(csLen);
        } else {
            usingSB = new StringBuilder(csLen);
        }
        BytesUtil.parseUtf8(in, usingSB, csLen);
        return usingSB;
    }

    @Override
    public CharSequenceSizedReader copy() {
        return INSTANCE;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        // no fields to read
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        // no fields to write
    }

    @Override
    public CharSequenceSizedReader readResolve() {
        return INSTANCE;
    }
}
