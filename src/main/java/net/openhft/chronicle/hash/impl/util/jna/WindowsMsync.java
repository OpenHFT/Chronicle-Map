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

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.platform.win32.BaseTSD.SIZE_T;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.Kernel32Util;
import com.sun.jna.win32.W32APIOptions;

import java.io.IOException;
import java.io.RandomAccessFile;

import static com.sun.jna.platform.win32.WinError.ERROR_LOCK_VIOLATION;

public final class WindowsMsync {

    private static final Kernel32Ex KERNEL_32 = (Kernel32Ex)
            Native.loadLibrary("kernel32", Kernel32Ex.class, W32APIOptions.UNICODE_OPTIONS);

    private WindowsMsync() {
    }

    public static void msync(RandomAccessFile raf, long addr, long length)
            throws IOException {
        int retry = 0;
        boolean success;
        int lastError = 0;
        // FlushViewOfFile can fail with ERROR_LOCK_VIOLATION if the memory system is writing dirty
        // pages to disk. As there is no way to synchronize the flushing then we retry a limited
        // number of times.
        do {
            success = KERNEL_32.FlushViewOfFile(new Pointer(addr), new SIZE_T(length));
            if (success || (lastError = KERNEL_32.GetLastError()) != ERROR_LOCK_VIOLATION)
                break;
            retry++;
        } while (retry < 3);

        if (success) {
            // Finally calls FlushFileBuffers
            raf.getChannel().force(false);
        } else {
            throw new IOException(Kernel32Util.formatMessageFromLastErrorCode(lastError));
        }
    }

    public interface Kernel32Ex extends Kernel32 {
        boolean FlushViewOfFile(Pointer addr, SIZE_T length);
    }
}
