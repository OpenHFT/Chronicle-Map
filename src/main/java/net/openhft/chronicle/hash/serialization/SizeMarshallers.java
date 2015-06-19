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

package net.openhft.chronicle.hash.serialization;

import net.openhft.lang.io.Bytes;

import static net.openhft.lang.io.IOTools.stopBitLength;

public final class SizeMarshallers {

    public static SizeMarshaller stopBit() {
        return StopBit.INSTANCE;
    }

    private enum StopBit implements SizeMarshaller {
        INSTANCE;

        @Override
        public int sizeEncodingSize(long size) {
            return stopBitLength(size);
        }

        @Override
        public long minEncodableSize() {
            return 0L;
        }

        @Override
        public int minSizeEncodingSize() {
            return 1;
        }

        @Override
        public int maxSizeEncodingSize() {
            return 9;
        }

        @Override
        public void writeSize(Bytes bytes, long size) {
            bytes.writeStopBit(size);
        }

        @Override
        public long readSize(Bytes bytes) {
            return bytes.readStopBit();
        }
    }

    public static SizeMarshaller constant(long size) {
        return new ConstantSizeMarshaller(size);
    }

    private static class ConstantSizeMarshaller implements SizeMarshaller {
        private static final long serialVersionUID = 0L;

        private final long size;

        private ConstantSizeMarshaller(long size) {
            this.size = size;
        }

        @Override
        public int sizeEncodingSize(long size) {
            return 0;
        }

        @Override
        public long minEncodableSize() {
            return size;
        }

        @Override
        public int minSizeEncodingSize() {
            return 0;
        }

        @Override
        public int maxSizeEncodingSize() {
            return 0;
        }

        @Override
        public void writeSize(Bytes bytes, long size) {
            // do nothing
        }

        @Override
        public long readSize(Bytes bytes) {
            return size;
        }
    }

    private SizeMarshallers() {}
}
