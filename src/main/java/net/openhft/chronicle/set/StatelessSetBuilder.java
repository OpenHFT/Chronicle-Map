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

package net.openhft.chronicle.set;

import net.openhft.chronicle.hash.StatelessClientBuilder;
import net.openhft.chronicle.map.ChronicleMap;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

final class StatelessSetBuilder<E> implements StatelessClientBuilder<ChronicleSet<E>> {

    private final StatelessClientBuilder<ChronicleMap<E, DummyValue>> mapClientBuilder;

    StatelessSetBuilder(StatelessClientBuilder<ChronicleMap<E, DummyValue>> mapClientBuilder) {
        this.mapClientBuilder = mapClientBuilder;
    }

    @Override
    public StatelessClientBuilder<ChronicleSet<E>> timeout(long timeout, TimeUnit units) {
        return new StatelessSetBuilder<>(mapClientBuilder.timeout(timeout, units));
    }

    @Override
    public ChronicleSet<E> create() throws IOException {
        return new SetFromMap<>(mapClientBuilder.create());
    }
}
