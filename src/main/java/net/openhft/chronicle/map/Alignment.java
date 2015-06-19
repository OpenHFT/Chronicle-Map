/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import net.openhft.lang.io.Bytes;

/**
 * Memory addresses alignment strategies.
 *
 * @see ChronicleMapBuilder#entryAndValueAlignment(Alignment)
 */
public enum Alignment {
    /**
     * Doesn't align memory addresses.
     */
    NO_ALIGNMENT {
        @Override
        void alignPositionAddr(Bytes bytes) {
            // no-op
        }

        @Override
        long alignAddr(long addr) {
            return addr;
        }

        @Override
        int alignSize(int size) {
            return size;
        }

        @Override
        int alignment() {
            return 1;
        }
    },

    /**
     * Aligns memory addresses on 4-byte boundary.
     */
    OF_4_BYTES {
        @Override
        void alignPositionAddr(Bytes bytes) {
            bytes.alignPositionAddr(4);
        }

        @Override
        long alignAddr(long addr) {
            return (addr + 3) & ~3;
        }

        @Override
        int alignSize(int size) {
            return (size + 3) & ~3;
        }

        @Override
        int alignment() {
            return 4;
        }
    },

    /**
     * Aligns memory addresses on 8-byte boundary.
     */
    OF_8_BYTES {
        @Override
        void alignPositionAddr(Bytes bytes) {
            bytes.alignPositionAddr(8);
        }

        @Override
        long alignAddr(long addr) {
            return (addr + 7) & ~7;
        }

        @Override
        int alignSize(int size) {
            return (size + 7) & ~7;
        }

        @Override
        int alignment() {
            return 8;
        }
    };

    private static final Alignment[] VALUES = values();

    static Alignment fromOrdinal(int ordinal) {
        return VALUES[ordinal];
    }

    abstract void alignPositionAddr(Bytes bytes);

    abstract long alignAddr(long addr);

    abstract int alignSize(int size);

    abstract int alignment();
}
