/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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

package net.openhft.chronicle.hash.impl;

import com.sun.jna.Native;
import com.sun.jna.Platform;

import java.io.IOException;

final class PosixMsync {

    private static final int MS_SYNC = 4;

    public static void msync(long addr, long length) throws IOException {
        if (msync(addr, length, MS_SYNC) == -1)
            throw new IOException(strerror_r(Native.getLastError(), null, 0));
    }

    private static native String strerror_r(int errnum, char[] buf, int buflen);

    private static native int msync(long addr, long length, int flags);

    static {
        Native.register(PosixMsync.class, Platform.C_LIBRARY_NAME);
    }

    private PosixMsync() {}
}
