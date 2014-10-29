/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.lang.io.AbstractBytes;
import net.openhft.lang.io.Bytes;

import java.io.ObjectStreamException;

public final class CharSequenceWriter<CS extends CharSequence> implements BytesWriter<CS> {
    private static final long serialVersionUID = 0L;
    private static final CharSequenceWriter INSTANCE = new CharSequenceWriter();

    public static <CS extends CharSequence> CharSequenceWriter<CS> instance() {
        return INSTANCE;
    }

    private CharSequenceWriter() {}

    @Override
    public long size(CS s) {
        return AbstractBytes.findUTFLength(s, s.length());
    }

    @Override
    public void write(Bytes bytes, CS s) {
        AbstractBytes.writeUTF0(bytes, s, s.length());
    }

    private Object readResolve() throws ObjectStreamException {
        return INSTANCE;
    }
}
