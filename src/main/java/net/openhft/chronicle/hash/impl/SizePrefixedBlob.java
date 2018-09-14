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
