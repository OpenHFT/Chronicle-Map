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

package net.openhft.chronicle.hash.locks;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * - Inter-process,
 * - unfair,
 * - busy-waiting,
 * - reentrant,
 * - single-thread (paradox? :) Once a lock object obtained, it shouldn't be stored in a field and
 *   accessed from multiple threads, instead of that, lock objects should be obtained in each thread
 *   separately, using the same call chain. This is because since the lock is inter-process, it
 *   anyway keep the syncronization state in shared memory, but restricting on-heap "view" of
 *   shared lock is beneficial form perf POV, e. g., fields of the on-heap lock object shouldn't
 *   be volatile.
 *  - doesn't support conditions.
 */
public interface InterProcessLock extends Lock {

    boolean isHeldByCurrentThread();

    /**
     * Conditions are not supported by inter-process locks.
     *
     * @return nothing, always throws an exception
     * @throws UnsupportedOperationException always
     */
    @NotNull
    @Override
    Condition newCondition();
}
