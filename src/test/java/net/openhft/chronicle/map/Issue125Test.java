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

import net.openhft.chronicle.core.OS;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Created by Peter Lawrey on 04/06/17.
 */
public class Issue125Test {
    @Test
    public void test() throws IOException {
        final File cacheRoot = new File(OS.TARGET + "/test.cm3");
        ChronicleMapBuilder<byte[], byte[]> shaToNodeBuilder =
                ChronicleMapBuilder.of(byte[].class, byte[].class)
//                        .name("bytes-to-bytes")
                        .entries(1000000).
                        averageKeySize(20).
                        averageValueSize(30);

        ChronicleMap<byte[], byte[]> shaToNode =
                shaToNodeBuilder.createPersistedTo(cacheRoot);

        shaToNode.put("1".getBytes(), "2".getBytes());

        shaToNodeBuilder.createPersistedTo(cacheRoot);

    }

}
