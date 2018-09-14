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