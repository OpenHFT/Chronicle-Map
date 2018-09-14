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

package net.openhft.chronicle.hash.impl.stage.hash;

import net.openhft.chronicle.core.Memory;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.hash.ChronicleHash;
import net.openhft.chronicle.hash.ChronicleHashClosedException;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.hash.impl.BigSegmentHeader.LOCK_TIMEOUT_SECONDS;

public abstract class ThreadLocalState {

    private static final Memory MEMORY = OS.memory();
    private static final long CONTEXT_LOCK_OFFSET;
    private static final int CONTEXT_UNLOCKED = 0;
    private static final int CONTEXT_LOCKED_LOCALLY = 1;
    private static final int CONTEXT_CLOSED = 2;

    static {
        try {
            Field contextLockField =
                    ThreadLocalState.class.getDeclaredField("contextLock");
            contextLockField.setAccessible(true);
            CONTEXT_LOCK_OFFSET = MEMORY.getFieldOffset(contextLockField);
        } catch (NoSuchFieldException e) {
            throw new AssertionError(e);
        }
    }

    public boolean iterationContextLockedInThisThread;
    private volatile int contextLock = CONTEXT_UNLOCKED;

    /**
     * Returns {@code true} if this is the outer context lock in this thread, {@code false} if this
     * is a nested context.
     */
    public boolean lockContextLocally(ChronicleHash<?, ?, ?, ?> hash) {
        // hash().isOpen() check guarantees no starvation of a thread calling chMap.close() and
        // trying to close this context by closeContext() method below, while the thread owning this
        // context frequently locks and unlocks it (e. g. in a loop). This is also the only check
        // for chMap openness during the whole context usage lifecycle.
        if (hash.isOpen() && MEMORY.compareAndSwapInt(this, CONTEXT_LOCK_OFFSET,
                CONTEXT_UNLOCKED, CONTEXT_LOCKED_LOCALLY)) {
            return true;
        } else {
            if (contextLock == CONTEXT_LOCKED_LOCALLY)
                return false;
            // Don't extract this hash().isOpen() and the one above, because they could return
            // different results: the first (above) could return true, the second (below) - false.
            if (contextLock == CONTEXT_CLOSED || !hash.isOpen())
                throw new ChronicleHashClosedException(hash);
            throw new AssertionError("Unknown context lock state: " + contextLock);
        }
    }

    public void unlockContextLocally() {
        // Ensure all reads from mapped memory are done before thread calling chronicleMap.close()
        // frees resources potentially unmapping some memory from where those reads are performed.
        MEMORY.loadFence();
        // Avoid volatile write to avoid expensive store-load barrier
        MEMORY.writeOrderedInt(this, CONTEXT_LOCK_OFFSET, CONTEXT_UNLOCKED);
    }

    public void closeContext(String chronicleHashIdentityString) {
        if (tryCloseContext())
            return;
        // Unless there are bugs in this codebase, it could happen that
        // contextLock == CONTEXT_CLOSED here only if closeContext() has succeed, and the subsequent
        // contextHolder.clear() has failed in ChronicleHashResources.closeContext(), though this is
        // hardly imaginable: contextHolder.clear() couldn't fail with OutOfMemoryError (because
        // there are no allocations in this method) and StackOverflowError (because in this case
        // closeContext() would fail with StackOverflowError before). But anyway it's probably
        // a good idea to make this check rather than not to make.
        if (contextLock == CONTEXT_CLOSED)
            return;
        // If first attempt of closing a context (i. e. moving from unused to closed state) failed,
        // it means that the context is still in use. If this context belongs to the current thread,
        // this is a bug, because we cannot "wait" until context is unused in the same thread:
        if (owner() == Thread.currentThread()) {
            throw new IllegalStateException(chronicleHashIdentityString +
                    ": Attempt to close a Chronicle Hash in the context " +
                    "of not yet finished query or iteration");
        }
        // If the context belongs to a different thread, wait until that thread finishes it's work
        // with the context:

        // Double the current timeout for segment locks "without timeout", that effectively
        // specifies maximum lock (hence context) holding time
        long timeoutMillis = TimeUnit.SECONDS.toMillis(LOCK_TIMEOUT_SECONDS) * 2;
        long lastTime = System.currentTimeMillis();
        do {
            if (tryCloseContext())
                return;
            // Unless there are bugs in this codebase, this should never happen. But anyway it's
            // probably a good idea to make this check rather than not to make.
            if (contextLock == CONTEXT_CLOSED)
                return;
            Thread.yield();
            long now = System.currentTimeMillis();
            if (now != lastTime) {
                lastTime = now;
                timeoutMillis--;
            }
        } while (timeoutMillis >= 0);
        throw new RuntimeException(chronicleHashIdentityString +
                ": Failed to close a context, belonging to the thread\n" +
                owner() + ", in the state: " + owner().getState() + "\n" +
                "Possible reasons:\n" +
                "- The context owner thread exited before closing this context. Ensure that you\n" +
                "always close opened Chronicle Map's contexts, the best way to do this is to use\n" +
                "try-with-resources blocks." +
                "- The context owner thread runs some context operation (e. g. a query) for\n" +
                "unexpectedly long time (at least " + LOCK_TIMEOUT_SECONDS + " seconds).\n" +
                "You should either redesign your logic to spend less time in Chronicle Map\n" +
                "contexts (recommended) or synchronize map.close() with queries externally,\n" +
                "so that close() is called only after all query operations finished.\n" +
                "- Iteration over a large Chronicle Map takes more than " + LOCK_TIMEOUT_SECONDS +
                " seconds.\n" +
                "In this case you should synchronize map.close() with iterations over the map\n" +
                "externally, so that close() is called only after all iterations are finished.\n" +
                "- This is a dead lock involving the context owner thread and this thread (from\n" +
                "which map.close() method is called. Make sure you always close Chronicle Map\n" +
                "contexts, preferably using try-with-resources blocks.");
    }

    private boolean tryCloseContext() {
        return MEMORY.compareAndSwapInt(this, CONTEXT_LOCK_OFFSET,
                CONTEXT_UNLOCKED, CONTEXT_CLOSED);
    }

    public abstract Thread owner();
}
