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
