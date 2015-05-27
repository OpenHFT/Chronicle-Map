/*
 * Copyright 2015 Higher Frequency Trading
 *
 *  http://www.higherfrequencytrading.com
 *  
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.openhft.chronicle.hash;

import net.openhft.chronicle.algo.hashing.LongHashFunction;

/**
 * Defines reasonable defaults for {@code Value}'s {@code equals()}, {@code hashCode()} and
 * {@code toString()}. They should be default implementations in the {@code Value} interface itself,
 * but Java 8 doesn't allow to override {@code Object}'s methods by default implementations
 * in interfaces.
 */
public abstract class AbstractValue<V, T> implements Value<V, T> {

    /**
     * Constructor for use by subclasses. 
     */
    protected AbstractValue() {}

    /**
     * Computes value's hash code by applying a hash function to {@code Value}'s <i>bytes</i>
     * representation.
     */
    @Override
    public int hashCode() {
        return (int) hash(LongHashFunction.city_1_1());
    }

    /**
     * Compares {@code Value}s' <i>bytes</i> representations.
     */
    @Override
    public boolean equals(Object obj) {
        return obj != null &&
                obj instanceof Value &&
                Value.bytesEquivalent(this, (Value<?, ?>) obj);
    }

    /**
     * Delegates to {@code Value}'s <i>object</i> {@code toString()}.
     */
    @Override
    public String toString() {
        return get().toString();
    }
}
