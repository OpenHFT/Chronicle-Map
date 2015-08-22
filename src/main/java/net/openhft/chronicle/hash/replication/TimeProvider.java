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

package net.openhft.chronicle.hash.replication;

import net.openhft.chronicle.hash.ChronicleHashBuilder;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.set.ChronicleSet;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * TimeProvider was aims to possibly later provide an optimization to
 * {@link System#currentTime()} on every call to replicated {@link ChronicleMap}
 * or {@link ChronicleSet}.
 *
 * <p>Subclasses should be immutable, because {@link ChronicleHashBuilder} doesn't make defensive
 * copies.
 *
 * @author Rob Austin.
 * @see ChronicleHashBuilder#timeProvider(TimeProvider)
 */
public abstract class TimeProvider implements Serializable {
    private static final long serialVersionUID = 0L;

    /**
     * Delegates {@link #currentTime()} to {@link System#currentTime()}.
     */
    public static final TimeProvider SYSTEM = new System();

    public abstract long currentTime();

    /**
     * {@code timeProvider.scale(System.currentTimeMillis(), TimeUnit.MILLISECONDS)} should
     * result to {@link #currentTime()}.
     */
    public abstract long scale(long time, TimeUnit unit);

    /**
     * {@code timeProvider.unscale(timeProvider.currentTime(), TimeUnit.NANOSECONDS)} should result
     * to something close to {@code System.nanoTime()}.
     */
    public abstract long unscale(long time, TimeUnit toUnit);

    private static class System extends TimeProvider {
        private static final long serialVersionUID = 1L;
        private static final long SCALE = 1000;

        @Override
        public long currentTime() {
            return java.lang.System.currentTimeMillis() * SCALE;
        }

        @Override
        public long scale(long time, TimeUnit unit) {
            return TimeUnit.MILLISECONDS.convert(time, unit) * SCALE;
        }

        @Override
        public long unscale(long time, TimeUnit toUnit) {
            return toUnit.convert(time, TimeUnit.MILLISECONDS) / SCALE;
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
            return SYSTEM;
        }
    }

}
