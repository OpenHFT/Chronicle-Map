/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
