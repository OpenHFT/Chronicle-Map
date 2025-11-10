//
// Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
//

package net.openhft.lang.values;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.DynamicallySized;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.wire.BytesInBinaryMarshallable;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by peter.lawrey on 23/04/2015.
 */
class MovingAverageArray extends BytesInBinaryMarshallable implements DynamicallySized {
    private final transient List<MovingAverageCompact> buffer = new ArrayList<>();
    private final List<MovingAverageCompact> values = new ArrayList<>();

    @Override
    public void readMarshallable(BytesIn<?> bytes) throws IORuntimeException {
        int len = Maths.toUInt31(bytes.readStopBit());
        values.clear();
        for (int i = 0; i < len; i++) {
            if (buffer.size() <= values.size()) {
                buffer.add(ObjectUtils.newInstance(MovingAverageCompact.class));
            }
            MovingAverageCompact next = buffer.get(i);
            next.readMarshallable(bytes);
            values.add(next);
        }
    }

    @Override
    public void writeMarshallable(BytesOut<?> bytes) {
        bytes.writeStopBit(values.size());
        for (int i = 0; i < values.size(); i++)
            values.get(i).writeMarshallable(bytes);
    }

    public void add(MovingAverageCompact movingAverageCompact) {
        values.add(movingAverageCompact);
    }

    public MovingAverageCompact get(int i) {
        return values.get(i);
    }

    public int size() {
        return values.size();
    }
}
