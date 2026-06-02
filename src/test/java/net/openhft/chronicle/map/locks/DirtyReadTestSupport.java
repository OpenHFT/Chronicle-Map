/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.locks;

import net.openhft.chronicle.core.values.LongValue;
import org.junit.Assert;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static net.openhft.chronicle.values.Values.newNativeReference;

final class DirtyReadTestSupport {
    static final long HOLD_MILLIS = 100;
    static final long AWAIT_MILLIS = 2_000;

    private DirtyReadTestSupport() {
    }

    static void prewarmGeneratedValueClasses() {
        newNativeReference(ChronicleStampedLockVOInterface.class);
        newNativeReference(BondVOInterface.class);
        newNativeReference(LongValue.class);
    }

    static void signal(CountDownLatch latch) {
        if (latch != null) {
            latch.countDown();
        }
    }

    static void await(CountDownLatch latch, String event) throws InterruptedException {
        Assert.assertTrue("Timed out waiting for " + event,
                latch.await(AWAIT_MILLIS, TimeUnit.MILLISECONDS));
    }

    static void awaitIfPresent(CountDownLatch latch, String event) throws InterruptedException {
        if (latch != null) {
            await(latch, event);
        }
    }

    static void awaitRelease(CountDownLatch releaseLatch) throws InterruptedException {
        if (releaseLatch == null) {
            Thread.sleep(HOLD_MILLIS);
        } else {
            releaseLatch.await(AWAIT_MILLIS, TimeUnit.MILLISECONDS);
        }
    }

    static void join(Thread thread) throws InterruptedException {
        thread.join(AWAIT_MILLIS);
        Assert.assertFalse("Timed out waiting for " + thread.getName(), thread.isAlive());
    }
}
