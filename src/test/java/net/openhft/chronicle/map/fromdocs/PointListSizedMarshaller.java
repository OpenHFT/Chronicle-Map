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

package net.openhft.chronicle.map.fromdocs;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.ReadResolvable;
import net.openhft.chronicle.hash.serialization.SizedReader;
import net.openhft.chronicle.hash.serialization.SizedWriter;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

public final class PointListSizedMarshaller
        implements SizedReader<List<Point>>, SizedWriter<List<Point>>,
        ReadResolvable<PointListSizedMarshaller> {

    static final PointListSizedMarshaller INSTANCE = new PointListSizedMarshaller();
    /**
     * A point takes 16 bytes in serialized form: 8 bytes for both x and y value
     */
    private static final long ELEMENT_SIZE = 16;

    private PointListSizedMarshaller() {
    }

    @Override
    public long size(@NotNull List<Point> toWrite) {
        return toWrite.size() * ELEMENT_SIZE;
    }

    @Override
    public void write(Bytes out, long size, @NotNull List<Point> toWrite) {
        toWrite.forEach(point -> {
            out.writeDouble(point.x);
            out.writeDouble(point.y);
        });
    }

    @NotNull
    @Override
    public List<Point> read(@NotNull Bytes in, long size, List<Point> using) {
        if (size % ELEMENT_SIZE != 0) {
            throw new IORuntimeException("Bytes size should be a multiple of " + ELEMENT_SIZE +
                    ", " + size + " read");
        }
        long listSizeAsLong = size / ELEMENT_SIZE;
        if (listSizeAsLong > Integer.MAX_VALUE) {
            throw new IORuntimeException("List size couldn't be more than " + Integer.MAX_VALUE +
                    ", " + listSizeAsLong + " read");
        }
        int listSize = (int) listSizeAsLong;
        if (using == null) {
            using = new ArrayList<>(listSize);
            for (int i = 0; i < listSize; i++) {
                using.add(null);
            }
        } else if (using.size() < listSize) {
            while (using.size() < listSize) {
                using.add(null);
            }
        } else if (using.size() > listSize) {
            using.subList(listSize, using.size()).clear();
        }
        for (int i = 0; i < listSize; i++) {
            Point point = using.get(i);
            if (point == null)
                using.set(i, point = new Point());
            point.x = in.readDouble();
            point.y = in.readDouble();
        }
        return using;
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        // no fields to write
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        // no fields to read
    }

    @Override
    public PointListSizedMarshaller readResolve() {
        return INSTANCE;
    }
}