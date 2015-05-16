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

import net.openhft.chronicle.bytes.*;
import net.openhft.chronicle.algo.hashing.LongHashFunction;
import org.jetbrains.annotations.Nullable;

/**
 * Dual bytes/object access to keys/values/elements.
 * 
 * <p>Bytes access: {@link #access()} + {@link #handle()} + {@link #offset()} + {@link #size()}.
 * 
 * <p>Object access: {@link #get()}. 
 * 
 * In most cases, each particular value wraps either some object or some bytes. Object is marshalled
 * to bytes lazily on demand, and bytes are lazily deserialized to object, accordingly. 
 *  
 * @param <V> type of the accessed objects
 * @param <T> type of the handle for bytes access
 */
public interface Value<V, T> {
    
    ReadAccess<T> access();

    T handle();

    long offset();

    long size();

    default long hash(LongHashFunction f) {
        return f.hash(handle(), access(), offset(), size());
    }

    default <T2> boolean equivalent(ReadAccess<T2> access, T2 handle, long offset) {
        return Access.equivalent(access(), handle(), offset(), access, handle, offset, size());
    }

    default <S, T2, A extends ReadAccess<T2>> boolean equivalent(
            Accessor<S, T2, A> accessor, S source, long index) {
        return equivalent(accessor.access(source), accessor.handle(source),
                accessor.offset(source, index));
    }

    default <T2> void writeTo(WriteAccess<T2> access, T2 handle, long offset) {
        Access.copy(access(), handle(), offset(), access, handle, offset, size());
    }

    default <T2> void writeTo(StreamingDataOutput<?, ?, T2> output) {
        long offset = output.accessPositionOffset();
        output.skip(size());
        writeTo(output.access(), output.accessHandle(), offset);
    }

    default <S, T2, A extends WriteAccess<T2>> void writeTo(
            Accessor<S, T2, A> accessor, S source, long index) {
        writeTo(accessor.access(source), accessor.handle(source), accessor.offset(source, index));
    }

    /**
     * Returns "cached" object, generally not eligible for using outside some context, or a block,
     * synchronized with locks, or lambda, etc.
     * 
     * If the {@code Value} is object wrapper -- this method just returns this object.  
     */
    V get();

    /**
     * Reads the object from the value's bytes, trying to reuse the given object
     * (might be {@code null}).
     */
    V getUsing(@Nullable V usingInstance);

    static <T1, T2> boolean bytesEquivalent(Value<?, T1> v1, Value<?, T2> v2) {
        if (v1.size() != v2.size())
            return false;
        return Access.equivalent(v1.access(), v1.handle(), v1.offset(),
                v2.access(), v2.handle(), v2.offset(), v1.size());
    }
}
