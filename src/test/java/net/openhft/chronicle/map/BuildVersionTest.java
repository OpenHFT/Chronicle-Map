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

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.impl.util.BuildVersion;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Rob Austin.
 */
public class BuildVersionTest {

    @Test
    public void test() throws IOException, InterruptedException {
        // checks that we always get a version
        Assert.assertNotNull(BuildVersion.version());
    }

    /**
     * check that the map records the version
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testVersion() throws IOException, InterruptedException {

        try (ChronicleMap<Integer, Double> expected = ChronicleMap.of(Integer.class, Double.class)
                .entries(1).create()) {
            expected.put(1, 1.0);

            String version = ((VanillaChronicleMap) expected).persistedDataVersion();
            Assert.assertNotNull(BuildVersion.version(), version);

        }
    }

}
