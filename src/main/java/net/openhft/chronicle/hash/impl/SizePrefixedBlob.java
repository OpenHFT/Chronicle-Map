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

package net.openhft.chronicle.hash.impl;

public final class SizePrefixedBlob {

    public static final int HEADER_OFFSET = 0;
    public static final int SIZE_WORD_OFFSET = 8;
    public static final int SELF_BOOTSTRAPPING_HEADER_OFFSET = 12;

    public static final int READY = 0;
    public static final int NOT_COMPLETE = 0x80000000;

    public static final int DATA = 0;
    public static final int META_DATA = 0x40000000;

    public static final int SIZE_MASK = (1 << 30) - 1;

    private SizePrefixedBlob() {
    }

    public static boolean isReady(int sizeWord) {
        return sizeWord > 0;
    }

    public static int extractSize(int sizeWord) {
        return sizeWord & SIZE_MASK;
    }
}
