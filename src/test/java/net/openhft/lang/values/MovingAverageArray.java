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

package net.openhft.lang.values;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.DynamicallySized;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.ObjectUtils;
import net.openhft.chronicle.wire.AbstractBytesMarshallable;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by peter.lawrey on 23/04/2015.
 */
public class MovingAverageArray extends AbstractBytesMarshallable implements DynamicallySized {
    private transient List<MovingAverageCompact> buffer = new ArrayList<>();
    private List<MovingAverageCompact> values = new ArrayList<>();

    @Override
    public void readMarshallable(BytesIn bytes) throws IORuntimeException {
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
    public void writeMarshallable(BytesOut bytes) {
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
