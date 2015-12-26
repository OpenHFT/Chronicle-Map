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
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class NanosecondPrecisionSystemTimeProvider extends TimeProvider {
    private static final long serialVersionUID = 0L;

    private static final AtomicLong lastTimeHolder = new AtomicLong();

    private static final NanosecondPrecisionSystemTimeProvider INSTANCE =
            new NanosecondPrecisionSystemTimeProvider();

    public static NanosecondPrecisionSystemTimeProvider instance() {
        return INSTANCE;
    }

    private NanosecondPrecisionSystemTimeProvider() {}

    @Override
    public long currentTime() {
        long now = MILLISECONDS.toNanos(System.currentTimeMillis());
        while (true) {
            long lastTime = lastTimeHolder.get();
            if (now <= lastTime)
                return lastTime;
            if (lastTimeHolder.compareAndSet(lastTime, now))
                return now;
        }
    }

    @Override
    public long systemTimeIntervalBetween(
            long earlierTimeNanos, long laterTimeNanos, TimeUnit systemTimeIntervalUnit) {
        long intervalNanos = laterTimeNanos - earlierTimeNanos;
        return systemTimeIntervalUnit.convert(intervalNanos, NANOSECONDS);
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
