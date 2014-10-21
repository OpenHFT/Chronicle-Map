/*
 * Copyright 2014 Higher Frequency Trading http://www.higherfrequencytrading.com
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

package net.openhft.chronicle.common.threadlocal;

import java.util.concurrent.atomic.AtomicBoolean;

public final class ThreadLocalCopies {

    private static ThreadLocal<ThreadLocalCopies> states = new ThreadLocal<ThreadLocalCopies>() {
        @Override
        protected ThreadLocalCopies initialValue() {
            return new ThreadLocalCopies();
        }
    };

    static ThreadLocalCopies get() {
        return states.get();
    }

    AtomicBoolean currentlyAccessed = new AtomicBoolean(false);
    Object[] table;
    int size = 0, sizeLimit, mask;

    private ThreadLocalCopies() {
        init(32); // 16 entries
    }

    void init(int doubledCapacity) {
        table = new Object[doubledCapacity];
        sizeLimit = doubledCapacity / 3; // 0.66 fullness
        mask = (doubledCapacity - 1) & ~1;
    }

    void rehash() {
        Object[] oldTab = this.table;
        int oldCapacity = oldTab.length;
        if (oldCapacity == (1 << 30))
            throw new IllegalStateException("Hash is full");
        init(oldCapacity << 1);
        int m = mask;
        Object[] tab = this.table;
        for (int oldI = 0; oldI < oldCapacity; oldI += 2) {
            Object id = oldTab[oldI];
            if (id != null) {
                int i = System.identityHashCode(id) & m;
                while (tab[i] != null) {
                    i = (i + 2) & m;
                }
                tab[i] = id;
                tab[i + 1] = oldTab[oldI + 1];
            }
        }
    }

    void postInsert() {
        if (++size > sizeLimit)
            rehash();
    }
}
