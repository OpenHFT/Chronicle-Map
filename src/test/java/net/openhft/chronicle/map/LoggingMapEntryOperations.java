/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
