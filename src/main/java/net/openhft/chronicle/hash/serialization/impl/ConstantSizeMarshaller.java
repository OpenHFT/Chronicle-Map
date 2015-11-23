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

package net.openhft.chronicle.hash.serialization.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.SizeMarshaller;

public final class ConstantSizeMarshaller implements SizeMarshaller {

    /** Config field */
    private final long constantSize;

    public ConstantSizeMarshaller(long constantSize) {
        this.constantSize = constantSize;
    }

    @Override
    public int storingLength(long size) {
        return 0;
    }

    @Override
    public long minStorableSize() {
        return constantSize;
    }

    @Override
    public long maxStorableSize() {
        return constantSize;
    }

    @Override
    public int minStoringLengthOfSizesInRange(long minSize, long maxSize) {
        return 0;
    }

    @Override
    public int maxStoringLengthOfSizesInRange(long minSize, long maxSize) {
        return 0;
    }

    @Override
    public void writeSize(Bytes out, long sizeToWrite) {
        // do nothing
    }

    @Override
    public long readSize(Bytes in) {
        return constantSize;
    }
}
