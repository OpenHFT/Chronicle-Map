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
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.bytes.RandomDataInput;
import net.openhft.chronicle.bytes.RandomDataOutput;
import org.jetbrains.annotations.Nullable;

import static net.openhft.chronicle.algo.bytes.Access.checkedRandomDataInputAccess;

/**
 * Dual bytes/object access to keys/values/elements.
 * 
 * <p>Bytes access: {@link #bytes()} + {@link #offset()} + {@link #size()}.
 * 
 * <p>Object access: {@link #get()}. 
 * 
 * <p>In most cases, each particular value wraps either some object or some bytes. Object
 * is marshalled to bytes lazily on demand, and bytes are lazily deserialized to object,
 * accordingly. 
 *  
 * @param <V> type of the accessed objects
 */
public interface Data<V> {

    RandomDataInput bytes();

    /**
     * Returns the offset to the value's bytes.
     */
    long offset();

    /**
     * Returns the size of the value's bytes.
     */
    long size();

    default long hash(LongHashFunction f) {
        return f.hash(bytes(), checkedRandomDataInputAccess(), offset(), size());
    }

    default boolean equivalent(RandomDataInput source, long sourceOffset) {
        return BytesUtil.bytesEqual(source, sourceOffset, bytes(), offset(), size());
    }

    default  void writeTo(RandomDataOutput target, long targetOffset) {
        target.write(targetOffset, bytes(), offset(), size());
    }

    /**
     * Returns "cached" object, generally not eligible for using outside some context, or a block,
     * synchronized with locks, or lambda, etc.
     * 
     * If the {@code Data} is object wrapper -- this method just returns this object.
     */
    V get();

    /**
     * Reads the object from the value's bytes, trying to reuse the given object
     * (might be {@code null}).
     */
    V getUsing(@Nullable V usingInstance);

    static boolean bytesEquivalent(Data<?> d1, Data<?> d2) {
        if (d1.size() != d2.size())
            return false;
        return BytesUtil.bytesEqual(d1.bytes(), d1.offset(), d2.bytes(), d2.offset(), d1.size());
    }
}
