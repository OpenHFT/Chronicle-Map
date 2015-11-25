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

package net.openhft.chronicle.map.fromdocs;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.impl.EnumMarshallable;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

enum LongPairArrayReader implements SizedReader<LongPair[]>, EnumMarshallable<LongPairArrayReader> {
    INSTANCE;

    @NotNull
    @Override
    public LongPair[] read(@NotNull Bytes in, long size, LongPair[] using) {
        if (size > Integer.MAX_VALUE * 16L)
            throw new IllegalStateException("LongPair[] size couldn't be " + (size / 16L));
        int resLen = (int) (size / 16L);
        LongPair[] res;
        if (using != null) {
            if (using.length == resLen) {
                res = using;
            } else {
                res = Arrays.copyOf(using, resLen);
            }
        } else {
            res = new LongPair[resLen];
        }
        for (int i = 0; i < resLen; i++) {
            LongPair pair = res[i];
            if (pair == null)
                res[i] = pair = new LongPair();
            pair.first = in.readLong();
            pair.second = in.readLong();
        }
        return res;
    }

    @Override
    public LongPairArrayReader readResolve() {
        return INSTANCE;
    }
}
