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

package net.openhft.chronicle.hash.replication;

import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.ChronicleHashBuilderPrivateAPI;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * {@code TimeProvider} encapsulates time measurement for {@link ChronicleHash} replication needs.
 * It is used to obtain timestamps of entry updates, and to determine, when deleted entries become
 * eligible for complete purge from the Chronicle Hash data store.
 *
 * @see ReplicableEntry#originTimestamp()
 * @see RemoteOperationContext#remoteTimestamp()
 * @see ChronicleHashBuilderPrivateAPI#removedEntryCleanupTimeout(long, TimeUnit)
 */
public final class TimeProvider {

    private static final AtomicLong lastTimeHolder = new AtomicLong();
    private static LongSupplier millisecondSupplier = System::currentTimeMillis;
    private TimeProvider() {
    }

    /**
     * Returns a non-decreasing number, assumed to be used as a "timestamp".
     * <p>
     * <p>Approximate system time interval between two calls of this method is retrievable via
     * {@link #systemTimeIntervalBetween(long, long, TimeUnit)}, applied to the returned values
     * from those {@code currentTime()} calls.
     * <p>
     * <p>Safe and scalable for concurrent use from multiple threads.
     *
     * @return the current timestamp
     */
    public static long currentTime() {
        long now = MILLISECONDS.toNanos(millisecondSupplier.getAsLong());
        while (true) {
            long lastTime = lastTimeHolder.get();
            if (now <= lastTime)
                return lastTime;
            if (lastTimeHolder.compareAndSet(lastTime, now))
                return now;
        }
    }

    /**
     * Returns system time interval (i. e. wall time interval) between two time values, taken using
     * {@link #currentTime()} method, with the highest possible precision, in the given time units.
     *
     * @param earlierTime            {@link #currentTime()} result, taken at some moment in the past (earlier)
     * @param laterTime              {@link #currentTime()} result, taken at some moment in the past, but later
     *                               than {@code earlierTime} was taken ("later" means there is a happens-before relationship
     *                               between the two subject {@code currentTime()} calls)
     * @param systemTimeIntervalUnit the time units to return system time interval in
     * @return wall time interval between the specified moments in the given time unit
     */
    public static long systemTimeIntervalBetween(
            long earlierTime, long laterTime, TimeUnit systemTimeIntervalUnit) {
        long intervalNanos = laterTime - earlierTime;
        return systemTimeIntervalUnit.convert(intervalNanos, NANOSECONDS);
    }

    static void overrideMillisecondSupplier(final LongSupplier millisecondSupplier) {
        TimeProvider.millisecondSupplier = millisecondSupplier;
    }
}