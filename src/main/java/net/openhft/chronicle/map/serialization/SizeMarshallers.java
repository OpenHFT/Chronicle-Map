/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
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

package net.openhft.chronicle.map.serialization;

import net.openhft.lang.io.Bytes;

public final class SizeMarshallers {

    public static SizeMarshaller stopBit() {
        return StopBit.INSTANCE;
    }

    private enum StopBit implements SizeMarshaller {
        INSTANCE;

        @Override
        public int sizeEncodingSize(long size) {
            if (size <= 127)
                return 1;
            // numberOfLeadingZeros is cheap intrinsic on modern CPUs
            // integral division is not... but there is no choice
            return ((70 - Long.numberOfLeadingZeros(size)) / 7);
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

    private SizeMarshallers() {}
}
