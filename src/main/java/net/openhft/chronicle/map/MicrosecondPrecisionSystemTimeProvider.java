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

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.replication.TimeProvider;

import java.io.ObjectStreamException;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class MicrosecondPrecisionSystemTimeProvider extends TimeProvider {
    private static final long serialVersionUID = 0L;

    private static final MicrosecondPrecisionSystemTimeProvider INSTANCE =
            new MicrosecondPrecisionSystemTimeProvider();

    public static MicrosecondPrecisionSystemTimeProvider instance() {
        return INSTANCE;
    }

    private MicrosecondPrecisionSystemTimeProvider() {}

    @Override
    public long currentTime() {
        return MILLISECONDS.toMicros(System.currentTimeMillis());
    }

    @Override
    public long systemTimeIntervalBetween(
            long earlierTimeMicros, long laterTimeMicros, TimeUnit systemTimeIntervalUnit) {
        long intervalMicros = laterTimeMicros - earlierTimeMicros;
        return systemTimeIntervalUnit.convert(intervalMicros, MICROSECONDS);
    }

    @Override
    public boolean equals(Object o) {
        return o != null && o.getClass() == getClass();
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    private Object readResolve() throws ObjectStreamException {
        return INSTANCE;
    }
}
