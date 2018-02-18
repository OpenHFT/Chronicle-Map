/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
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
