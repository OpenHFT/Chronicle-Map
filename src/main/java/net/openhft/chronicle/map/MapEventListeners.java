/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
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

package net.openhft.chronicle.map;


import net.openhft.lang.io.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MapEventListeners {

    /**
     * Factories a more flexible than public static instances.
     * We can add some configuration-on-the-first-call in the future.
     */

    static final MapEventListener NOP = new MapEventListener() {
        private static final long serialVersionUID = 0L;
    };
    private static final MapEventListener BYTES_LOGGING = new MapEventListener() {
        private static final long serialVersionUID = 0L;
        public final Logger LOGGER = LoggerFactory.getLogger(getClass());

        @Override
        public void onGetFound(ChronicleMap map, Bytes entry, int metaDataBytes,
                               Object key, Object value) {
            logOperation(map, entry, metaDataBytes, " get ");
        }

        private void logOperation(ChronicleMap map, Bytes entry, int metaDataBytes, String oper) {
            StringBuilder sb = new StringBuilder();
            sb.append(map.file()).append(oper);
            if (metaDataBytes > 0) {
                sb.append("Meta: ");
                entry.toString(sb, 0L, 0L, metaDataBytes);
                sb.append(" | ");
            }
            Bytes slice = entry.slice(metaDataBytes, entry.limit() - metaDataBytes);
            VanillaChronicleMap vanillaMap = (VanillaChronicleMap) map;
            long keySize = vanillaMap.keySizeMarshaller.readSize(slice);
            slice.toString(sb, slice.position(), 0L, slice.position() + keySize);
            slice.position(slice.position() + keySize);
            long valueSize = vanillaMap.valueSizeMarshaller.readSize(slice);
            vanillaMap.alignment.alignPositionAddr(slice);
            sb.append(" = ");
            slice.toString(sb, slice.position(), 0L, slice.position() + valueSize);
            LOGGER.info(sb.toString());
        }

        @Override
        public void onPut(ChronicleMap map, Bytes entry, int metaDataBytes, boolean added,
                          Object key, Object value, long pos, SharedSegment segment) {
            logOperation(map, entry, metaDataBytes, added ? " +put " : " put ");
        }

        @Override
        public void onRemove(ChronicleMap map, Bytes entry, int metaDataBytes,
                             Object key, Object value, long pos, SharedSegment segment) {
            logOperation(map, entry, metaDataBytes, " remove ");
        }
    };
    private static final MapEventListener KEY_VALUE_LOGGING = new MapEventListener() {
        private static final long serialVersionUID = 0L;
        public final Logger LOGGER = LoggerFactory.getLogger(getClass());

        @Override
        public void onGetFound(ChronicleMap map, Bytes entry, int metaDataBytes,
                               Object key, Object value) {
            LOGGER.info("{} get {} => {}", map.file(), key, value);
        }

        @Override
        public void onPut(ChronicleMap map, Bytes entry, int metaDataBytes, boolean added,
                          Object key, Object value, long pos, SharedSegment segment) {
            LOGGER.info("{} put {} => {}", map.file(), key, value);
        }

        @Override
        public void onRemove(ChronicleMap map, Bytes entry, int metaDataBytes,
                             Object key, Object value, long pos, SharedSegment segment) {
            LOGGER.info("{} remove {} was {}", map.file(), key, value);
        }
    };

    private MapEventListeners() {
    }

    @SuppressWarnings("unchecked")
    public static <K, V, M extends ChronicleMap<K, V>> MapEventListener<K, V, M> nop() {
        return NOP;
    }

    @SuppressWarnings("unchecked")
    public static <K, V, M extends ChronicleMap<K, V>> MapEventListener<K, V, M> bytesLogging() {
        return BYTES_LOGGING;
    }

    @SuppressWarnings("unchecked")
    public static <K, V, M extends ChronicleMap<K, V>> MapEventListener<K, V, M> keyValueLogging() {
        return KEY_VALUE_LOGGING;
    }
}
