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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import net.openhft.chronicle.hash.hashing.Hasher;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

public class HasherTest {
    public static void main(String[] args) {

        HashFunction hashFunction = Hashing.murmur3_128();
        for (int i=0;i<10;i++)
        {
            String key = "010758403"+String.format("%06d",i)+"S-INJFIX_SLE";
            long hashCode = Hasher.hash(key.getBytes(ISO_8859_1));
            long guavaHashCode = hashFunction.hashBytes(key.getBytes(ISO_8859_1)).asLong();
            System.out.println("<"+key+"> => "+hashCode+" "+guavaHashCode);
        }
    }
}
