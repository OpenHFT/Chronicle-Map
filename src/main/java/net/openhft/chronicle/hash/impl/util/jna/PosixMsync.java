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

package net.openhft.chronicle.hash.impl.util.jna;

import com.sun.jna.*;

import java.io.IOException;

public final class PosixMsync {

    private static final int MS_SYNC = 4;

    static {
        NativeLibrary clib = NativeLibrary.getInstance(Platform.C_LIBRARY_NAME);
        Native.register(PosixMsync.class, clib);
    }

    private PosixMsync() {
    }

    public static void msync(long addr, long length) throws IOException {
        if (msync(new Pointer(addr), new size_t(length), MS_SYNC) == -1)
            throw new IOException("msync failed: error code " + Native.getLastError());
    }

    private static native int msync(Pointer addr, size_t length, int flags);

    public static class size_t extends IntegerType {
        private static final long serialVersionUID = 0L;

        // no-arg constructor is required by JNA
        @SuppressWarnings("unused")
        public size_t() {
            this(0);
        }

        public size_t(long value) {
            super(Native.SIZE_T_SIZE, value, true);
        }
    }
}
