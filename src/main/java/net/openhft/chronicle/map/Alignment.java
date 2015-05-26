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
        public void alignPositionAddr(Bytes bytes) {
            // no-op
        }

        @Override
        public long alignAddr(long addr) {
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
        public void alignPositionAddr(Bytes bytes) {
            bytes.alignPositionAddr(4);
        }

        @Override
        public long alignAddr(long addr) {
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
        public void alignPositionAddr(Bytes bytes) {
            bytes.alignPositionAddr(8);
        }

        @Override
        public long alignAddr(long addr) {
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

    public abstract void alignPositionAddr(Bytes bytes);

    public abstract long alignAddr(long addr);

    abstract int alignSize(int size);

    abstract int alignment();
}
