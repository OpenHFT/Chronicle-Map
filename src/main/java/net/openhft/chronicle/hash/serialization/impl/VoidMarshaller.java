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

import net.openhft.chronicle.hash.serialization.AgileBytesMarshaller;
import net.openhft.lang.io.Bytes;

public enum VoidMarshaller implements AgileBytesMarshaller<Void> {
    INSTANCE;

    @Override
    public long size(Void e) {
        return 0L;
    }

    @Override
    public int sizeEncodingSize(long size) {
        return 0;
    }

    @Override
    public void writeSize(Bytes bytes, long size) {
        // do nothing
    }

    @Override
    public boolean startsWith(Bytes bytes, Void e) {
        return false;
    }

    @Override
    public long hash(Void e) {
        return 0;
    }

    @Override
    public void write(Bytes bytes, Void e) {
        // do nothing;
    }

    @Override
    public long readSize(Bytes bytes) {
        return 0L;
    }

    @Override
    public Void read(Bytes bytes, long size) {
        // Void nothing;
        return null;
    }

    @Override
    public Void read(Bytes bytes, long size, Void aVoid) {
        return null;
    }

}
