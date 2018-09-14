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

import net.openhft.chronicle.hash.Data;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LoggingMapEntryOperations<K, V> implements MapEntryOperations<K, V, Void> {

    static final Logger LOG = LoggerFactory.getLogger(LoggingMapEntryOperations.class);

    @Override
    public Void replaceValue(@NotNull MapEntry<K, V> entry, Data<V> newValue) {
        LOG.info("replace: old key: {}, old value: {}, new value: {}",
                entry.key(), entry.value(), newValue);
        entry.doReplaceValue(newValue);
        return null;
    }
}
