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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import net.openhft.chronicle.hash.hashing.Hasher;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HasherTest {
    @Test
    public void test() {
        HashFunction hashFunction = Hashing.murmur3_128();
        for (int i=0;i<10;i++)
        {
            String key = "010758403"+String.format("%06d",i)+"S-INJFIX_SLE";
            long hashCode = Hasher.hash(key.getBytes());
            long guavaHashCode = hashFunction.hashBytes(key.getBytes()).asLong();
            assertEquals(guavaHashCode, hashCode);
        }
    }
}
