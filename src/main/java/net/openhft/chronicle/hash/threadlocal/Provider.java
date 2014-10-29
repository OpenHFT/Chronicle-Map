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

package net.openhft.chronicle.hash.threadlocal;

public abstract class Provider<T> {

    public static <T> Provider<T> of(Class<T> tClass) {
        if (StatefulCopyable.class.isAssignableFrom(tClass)) {
            return StatefulProvider.INSTANCE;
        }
        return StatelessProvider.INSTANCE;
    }

    public abstract T get(ThreadLocalCopies copies, T original);

    public abstract ThreadLocalCopies getCopies(ThreadLocalCopies copies);

    private static final class StatefulProvider<T extends StatefulCopyable<T>> extends Provider<T> {
        private static final Provider INSTANCE = new StatefulProvider();

        @Override
        public ThreadLocalCopies getCopies(ThreadLocalCopies copies) {
            if (copies != null)
                return copies;
            return ThreadLocalCopies.get();
        }

        @Override
        public T get(ThreadLocalCopies copies, T original) {
            if (copies.currentlyAccessed.compareAndSet(false, true)) {
                Object id = original.stateIdentity();
                int m = copies.mask;
                Object[] tab = copies.table;
                int i = System.identityHashCode(id) & m;
                T copy;
                while (true) {
                    Object idInTable = tab[i];
                    if (idInTable == id) {
                        copy = (T) tab[i + 1];
                        break;
                    } else if (idInTable == null) {
                        tab[i] = id;
                        tab[i + 1] = copy = original.copy();
                        copies.postInsert();
                        break;
                    }
                    i = (i + 2) & m;
                }
                copies.currentlyAccessed.set(false);
                return copy;
            } else {
                throw new IllegalStateException(
                        "Concurrent or recursive access to ThreadLocalCopies is not allowed");
            }
        }
    }

    private static final class StatelessProvider<M> extends Provider<M> {
        private static final Provider INSTANCE = new StatelessProvider();

        @Override
        public M get(ThreadLocalCopies copies, M original) {
            return original;
        }

        @Override
        public ThreadLocalCopies getCopies(ThreadLocalCopies copies) {
            return copies;
        }
    }
}
