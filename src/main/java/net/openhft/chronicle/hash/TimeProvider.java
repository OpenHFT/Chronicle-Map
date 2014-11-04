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

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.set.ChronicleSet;

import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * TimeProvider was aims to possibly later provide an optimization to
 * {@link System#currentTimeMillis()} on every call to replicated {@link ChronicleMap}
 * or {@link ChronicleSet}.
 *
 * Subclasses should be immutable, because {@link ChronicleHashBuilder} doesn't make defensive
 * copies.
 *
 * @author Rob Austin.
 * @see ChronicleHashBuilder#timeProvider(TimeProvider)
 */
public abstract class TimeProvider implements Serializable {
    /**
     * Delegates {@link #currentTimeMillis()} to {@link System#currentTimeMillis()}.
     */
    public static final TimeProvider SYSTEM = new System();
    private static final long serialVersionUID = 0L;

    public abstract long currentTimeMillis();

    private static class System extends TimeProvider {
        private static final long serialVersionUID = 0L;

        public long currentTimeMillis() {
            return java.lang.System.currentTimeMillis();
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
