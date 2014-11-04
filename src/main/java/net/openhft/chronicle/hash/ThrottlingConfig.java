/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.hash;


import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


public class ThrottlingConfig {

    private static final long DEFAULT_BUCKET_INTERVAL = 100L;
    private static final TimeUnit DEFAULT_BUCKET_INTERVAL_UNIT = MILLISECONDS;

    private static final ThrottlingConfig NO_THROTTLING = create(0L, MILLISECONDS,
            DEFAULT_BUCKET_INTERVAL, DEFAULT_BUCKET_INTERVAL_UNIT);

    private final long throttle;
    private final TimeUnit throttlePerUnit;
    private final long bucketInterval;
    private final TimeUnit bucketIntervalUnit;


    ThrottlingConfig(
            long throttle,
            TimeUnit throttlePerUnit,
            long bucketInterval,
            TimeUnit bucketIntervalUnit) {
        this.throttle = throttle;
        if (throttlePerUnit == null) {
            throw new NullPointerException("Null throttlePerUnit");
        }
        this.throttlePerUnit = throttlePerUnit;
        this.bucketInterval = bucketInterval;
        if (bucketIntervalUnit == null) {
            throw new NullPointerException("Null bucketIntervalUnit");
        }
        this.bucketIntervalUnit = bucketIntervalUnit;
    }

    /**
     * Returns a config which prescribe that a replicator shouldn't throttle.
     *
     * @return a config which prescribe that a replicator shouldn't throttle
     */
    public static ThrottlingConfig noThrottling() {
        return NO_THROTTLING;
    }

    /**
     * Returns a config which throttle the specified max bits per the given time unit with default bucketing
     * interval.
     *
     * @param maxBits the preferred maximum bits
     * @param perUnit the time unit per which maximum bits specified
     * @return a config which throttle the specified max bits per the given time unit
     * @throws IllegalArgumentException if the specified {@code maxBits} is non-positive value
     */
    public static ThrottlingConfig throttle(long maxBits, TimeUnit perUnit) {
        if (maxBits <= 0L)
            throw new IllegalArgumentException();
        return create(maxBits, perUnit, DEFAULT_BUCKET_INTERVAL, DEFAULT_BUCKET_INTERVAL_UNIT);
    }

    public static void checkMillisecondBucketInterval(ThrottlingConfig config, String replicator) {
        if (config.bucketInterval(MILLISECONDS) < 1) {
            throw new IllegalArgumentException(
                    "Minimum throttle bucketing interval of 1 millisecond is supported by " +
                            replicator + " replicator");
        }
    }

    static ThrottlingConfig create(
            long throttle, TimeUnit throttlePerUnit,
            long throttleBucketInterval, TimeUnit throttleBucketIntervalUnit) {
        return new ThrottlingConfig(throttle, throttlePerUnit,
                throttleBucketInterval, throttleBucketIntervalUnit);
    }


    /**
     * Returns maximum bits per the given time unit, i. e. the throttling. {@code 0} (zero) designates there
     * is no throttling.
     *
     * @param perUnit maximum bits is returned per this time unit
     * @return maximum bits per the given time unit, or {@code 0} if there is no throttling
     */
    public final long throttling(TimeUnit perUnit) {
        return throttlePerUnit.convert(throttle, perUnit);
    }


    /**
     * Returns the throttle bucketing interval in the given time units.  <p>Default throttle bucketing
     * interval is 100 millis.
     *
     * @param unit the time unit of the interval
     * @return the bucketing interval for throttling in the given time units
     */
    public long bucketInterval(TimeUnit unit) {
        return unit.convert(bucketInterval, bucketIntervalUnit);
    }

    /**
     * Returns a copy of this config with the specified bucketing interval.
     *
     * @param throttleBucketInterval the bucketing interval for throttling
     * @param unit                   the time unit of the interval
     * @return a copy of this config with the specified bucketing interval
     * @throws IllegalArgumentException if the given bucketing interval is non-positive
     */
    public ThrottlingConfig bucketInterval(long throttleBucketInterval, TimeUnit unit) {
        if (throttleBucketInterval <= 0L)
            throw new IllegalArgumentException();
        return create(throttle, throttlePerUnit, throttleBucketInterval, unit);
    }

    @Override
    public String toString() {
        return "ThrottlingConfig{"
                + "throttle=" + throttle
                + ", throttlePerUnit=" + throttlePerUnit
                + ", bucketInterval=" + bucketInterval
                + ", bucketIntervalUnit=" + bucketIntervalUnit
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof ThrottlingConfig) {
            ThrottlingConfig that = (ThrottlingConfig) o;
            return (this.throttle == that.throttle)
                    && (this.throttlePerUnit.equals(that.throttlePerUnit))
                    && (this.bucketInterval == that.bucketInterval)
                    && (this.bucketIntervalUnit.equals(that.bucketIntervalUnit));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= (throttle >>> 32) ^ throttle;
        h *= 1000003;
        h ^= throttlePerUnit.hashCode();
        h *= 1000003;
        h ^= (bucketInterval >>> 32) ^ bucketInterval;
        h *= 1000003;
        h ^= bucketIntervalUnit.hashCode();
        return h;
    }
}
