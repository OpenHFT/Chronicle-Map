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

package net.openhft.chronicle.hash.serialization.impl;

/**
 * @deprecated this class exists for compatibility with old versions of Chronicle Map, which used
 * wrong version of xxHash.
 */
@Deprecated
public final class WrongXxHash {

    // xxHash constants
    private static final long P1 = -7046029288634856825L;
    private static final long P2 = -4417276706812531889L;
    private static final long P3 = 1609587929392839161L;
    private static final long P5 = 2870177450012600261L;

    private WrongXxHash() {
    }

    public static long hashInt(int input) {
        long hash = P5 + 4;
        // Preserve old, wrong behaviour. In the correct version, input is widened to long as
        // unsigned here.
        hash ^= input * P1;
        hash = Long.rotateLeft(hash, 23) * P2 + P3;
        return finalize(hash);
    }

    private static long finalize(long hash) {
        hash ^= hash >>> 33;
        hash *= P2;
        hash ^= hash >>> 29;
        hash *= P3;
        hash ^= hash >>> 32;
        return hash;
    }
}
