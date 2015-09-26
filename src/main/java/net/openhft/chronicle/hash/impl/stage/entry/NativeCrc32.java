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

package net.openhft.chronicle.hash.impl.stage.entry;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.zip.CRC32;

public enum NativeCrc32 implements Crc32 {
    INSTANCE;

    static final MethodHandle updateByteBuffer;
    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            updateByteBuffer = lookup.findStatic(CRC32.class, "updateByteBuffer",
                    MethodType.methodType(int.class, long.class, int.class, int.class));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int crc32(long addr, long len) {
        if (len > Integer.MAX_VALUE)
            throw new UnsupportedOperationException("Native Crc32 checksum doesn't support " +
                    "entries bigger than 2^31 - 1 in size, " + len + " given");
        try {
            return (int) updateByteBuffer.invoke(addr, 0, (int) len);
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }
}
