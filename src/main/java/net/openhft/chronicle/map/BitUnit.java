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

package net.openhft.chronicle.map;

/**
 * @author Rob Austin.
 */
public enum BitUnit {

    BITS {
        public long toBits(long v) {
            return v;
        }

        public long toMegaBits(long v) {
            return v / _MB;
        }

        public long toGigaBits(long v) {
            return v / _GB;
        }
    },

    MEGA_BITS {
        public long toBits(long v) {
            return v * _MB;
        }

        public long toMegaBits(long v) {
            return v;
        }

        public long toGigaBits(long v) {
            return v / (_GB / _MB);
        }
    },

    GIGA_BITS {
        public long toBits(long v) {
            return v * _GB;
        }

        public long toMegaBits(long v) {
            return v * (_GB / _MB);
        }

        public long toGigaBits(long v) {
            return v;
        }
    };
    private static long _KB = 1000;
    private static long _MB = 1000 * _KB;
    private static long _GB = 1000 * _MB;

    public long toBits(long duration) {
        throw new AbstractMethodError();
    }

    long toMegaBits(long value) {
        throw new AbstractMethodError();
    }

    public long toGigaBits(long v) {
        throw new AbstractMethodError();
    }
}
