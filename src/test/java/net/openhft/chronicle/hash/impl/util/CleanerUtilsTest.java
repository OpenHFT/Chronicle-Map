package net.openhft.chronicle.hash.impl.util;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class CleanerUtilsTest {

    @Test
    public void testClean() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Object ob = new Object();
        Cleaner cleaner = CleanerUtils.createCleaner(ob, latch::countDown);

        cleaner.clean();

        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

}