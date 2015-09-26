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

public interface Crc32 {

    int crc32(long addr, long len);

    enum Crc32Impl {
        ;

        static Crc32 crc32;
        static {
            try {
                crc32 = NativeCrc32.INSTANCE;
            } catch (Throwable e) {
                // ignore
            } finally {
                if (crc32 == null)
                    crc32 = FallbackJavaCrc32.INSTANCE;
            }
        }
    }

    static int compute(long addr, long len) {
        return Crc32Impl.crc32.crc32(addr, len);
    }
}
