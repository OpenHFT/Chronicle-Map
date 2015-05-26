/*
 * Copyright 2015 Higher Frequency Trading
 *
 *  http://www.higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.bytes.Access;
import net.openhft.chronicle.bytes.Accessor;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.NativeBytes;

import static net.openhft.chronicle.bytes.Access.nativeAccess;

public final class JavaLangBytesAccessors {

    public static Accessor.Full<Bytes, ?> uncheckedBytesAccessor(Bytes bytes) {
        return bytes instanceof NativeBytes ? NativeBytesAccessor.INSTANCE :
                GenericBytesAccessor.INSTANCE;
    }

    enum NativeBytesAccessor implements Accessor.Full<Bytes, Void> {
        INSTANCE;

        @Override
        public Access<Void> access(Bytes source) {
            return nativeAccess();
        }

        @Override
        public Void handle(Bytes source) {
            return null;
        }

        @Override
        public long offset(Bytes source, long index) {
            return source.address() + index;
        }
    }

    enum GenericBytesAccessor implements Accessor.Full<Bytes, Bytes> {
        INSTANCE;

        @Override
        public Access<Bytes> access(Bytes source) {
            return JavaLangBytesAccess.INSTANCE;
        }

        @Override
        public Bytes handle(Bytes source) {
            return source;
        }

        @Override
        public long offset(Bytes source, long index) {
            return index;
        }
    }

    private JavaLangBytesAccessors() {}
}
