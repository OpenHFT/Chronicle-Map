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

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.ChronicleHashBuilder;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * TimeProvider abstracts time measurement for {@link ChronicleHash} replication needs. {@code
 * TimeProvider} is specified to be used in a replicated {@code ChronicleHash} via {@link
 * ChronicleHashBuilder#timeProvider(TimeProvider)} method. The simplest (including default)
 * providers rely on system time ({@link System#currentTimeMillis()}), but more complex ones could
 * use some atomic counters, or different sources of time.
 *
 * <p>Possible reasons to implement a custom time provider:
 * <ul>
 *     <li>To synchronize time between replicated nodes, when precision of the system time is
 *     not sufficient (it could diverge on remote servers).</li>
 *     <li>Optimization, if {@code System.currentTimeMillis()} is considered too slow</li>
 *     <li>If time machinery in replicated {@code ChronicleHash} system is completely hijacked,
 *     and used to store some {@code long} value with non-time meaning (see {@link
 *     ReplicableEntry#originTimestamp()}, {@link ReplicableEntry#updateOrigin(byte, long)},
 *     {@link RemoteOperationContext#remoteTimestamp()}).</li>
 * </ul>
 *
 * <p>Subclasses should be immutable, because {@link ChronicleHashBuilder} doesn't make defensive
 * copies.
 *
 * @see ChronicleHashBuilder#timeProvider(TimeProvider)
 */
public abstract class TimeProvider implements Serializable {
    private static final long serialVersionUID = 0L;

    /**
     * Returns the current time in this provider's context.
     */
    public abstract long currentTime();

    /**
     * Returns system time interval (i. e. wall time interval) between two time values, taken using
     * this {@code TimeProvider}'s {@link #currentTime()} method, with the highest possible
     * precision, in the given time units.
     *
     * @param earlierTime {@link #currentTime()} result, taken at some moment in the past (earlier)
     * @param laterTime {@link #currentTime()} result, taken at some moment in the past, but later
     * than {@code earlierTime} was taken
     * @param systemTimeIntervalUnit the time units to return system time interval in
     * @return wall time interval between the specified moments in the given time unit
     */
    public abstract long systemTimeIntervalBetween(
            long earlierTime, long laterTime, TimeUnit systemTimeIntervalUnit);

}
