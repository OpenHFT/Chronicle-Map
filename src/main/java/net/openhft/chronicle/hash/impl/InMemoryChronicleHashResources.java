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

package net.openhft.chronicle.hash.impl;

import net.openhft.chronicle.core.OS;

import static net.openhft.chronicle.assertions.AssertUtil.SKIP_ASSERTIONS;
import static net.openhft.chronicle.map.internal.InternalAssertUtil.assertAddress;
import static net.openhft.chronicle.map.internal.InternalAssertUtil.assertPosition;

public final class InMemoryChronicleHashResources extends ChronicleHashResources {
    @Override
    void releaseMemoryResource(final MemoryResource allocation) {
        assert SKIP_ASSERTIONS || assertAddress(allocation.address);
        assert SKIP_ASSERTIONS || assertPosition(allocation.size);
        OS.memory().freeMemory(allocation.address, allocation.size);
    }
}
