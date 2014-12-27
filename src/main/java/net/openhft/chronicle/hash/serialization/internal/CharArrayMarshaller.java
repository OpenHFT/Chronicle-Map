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

package net.openhft.chronicle.hash.serialization.internal;

import net.openhft.chronicle.hash.hashing.Hasher;
import net.openhft.chronicle.hash.serialization.BytesInterop;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.lang.io.Bytes;

public enum CharArrayMarshaller implements BytesInterop<char[]>, BytesReader<char[]> {
    INSTANCE;

    @Override
    public boolean startsWith(Bytes bytes, char[] chars) {
        if (bytes.capacity() - bytes.position() < chars.length * 2L)
            return false;
        long pos = bytes.position();
        for (int i = 0; i < chars.length; i++) {
            if (bytes.readChar(pos + (i * 2L)) != chars[i])
                return false;
        }
        return true;
    }

    @Override
    public long hash(char[] chars) {
        return Hasher.hash(chars);
    }

    @Override
    public char[] read(Bytes bytes, long size) {
        char[] chars = new char[resLen(size)];
        bytes.readFully(chars);
        return chars;
    }

    @Override
    public char[] read(Bytes bytes, long size, char[] toReuse) {
        int resLen = resLen(size);
        if (toReuse == null || toReuse.length != resLen)
            toReuse = new char[resLen];
        bytes.readFully(toReuse);
        return toReuse;
    }

    private int resLen(long size) {
        long resLen = size / 2L;
        if (resLen < 0 || resLen > Integer.MAX_VALUE)
            throw new IllegalArgumentException("char[] size should be non-negative int, " +
                    resLen + " given. Memory corruption?");
        return (int) resLen;
    }

    @Override
    public long size(char[] chars) {
        return chars.length * 2L;
    }

    @Override
    public void write(Bytes bytes, char[] chars) {
        bytes.write(chars);
    }
}
