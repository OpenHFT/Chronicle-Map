/*
 *     Copyright (C) 2015  higherfrequencytrading.com
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.ChronicleHashBuilder;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Contains methods which are called when {@link ChronicleMap} key events occur. Typical use cases:
 * <ul> <li>Map data backup / replication.</li> <li>Logging, monitoring, debugging.</li> </ul>
 *
 * <p>This is an adapter class - all methods have default implementations as no-ops. Extend this
 * class and override only methods corresponding the events you are interested in.
 *
 * <p>To configure {@code MapEventListener} for {@code ChronicleMap}, use {@link
 * ChronicleMapBuilder#eventListener(MapEventListener)} method.
 *
 * <p>See {@link #logging(String)} implementation.
 *
 * <p>All these calls are synchronous while a {@code ChronicleMap} lock is held so make them as
 * quick as possible.
 *
 * <p>The {@code entry} passed to the methods is {@code Bytes} instance positioned at meta data
 * area. See {@link ChronicleHashBuilder#metaDataBytes(int)} for more information.
 *
 * @param <K> key type of the maps, trackable by this event listener
 * @param <V> value type of the maps, trackable by this event listener
 * @see ChronicleMapBuilder#eventListener(MapEventListener)
 */
public abstract class MapEventListener<K, V> implements Serializable {
    private static final long serialVersionUID = 0L;

    /**
     * Returns the map event listener, which logs strings like "{@linkplain ChronicleMap#file() map
     * file} opType key value", where opType is either "get", "put" or "remove", to the logger
     * provided by SLF4J.
     *
     * @param <K>    the map key type
     * @param <V>    the map value type
     * @param prefix logging prefix
     * @return the logging event listener
     */
    public static <K, V> MapEventListener<K, V> logging(String prefix) {
        //noinspection unchecked
        return new LoggingMapEventListener(prefix);
    }

    /**
     * This method is called whenever a new value is put for the key in the map during calls of such
     * methods as {@link ChronicleMap#put put}, {@link ChronicleMap#putIfAbsent putIfAbsent}, {@link
     * ChronicleMap#replace(Object, Object, Object) replace}, etc. When a new value is {@linkplain
     * ChronicleMapBuilder#defaultValue(Object) default} for the map or obtained during {@link
     * ChronicleMap#acquireUsing acquireUsing} call is put for the key, this method is called as
     * well.
     *
     * <p>This method is called when put is already happened.
     *
     * @param key                the key the given value is put for
     * @param newValue           the value which is now associated with the given key
     * @param replacedValue      the value which was replaced by {@code newValue}, {@code null} if
     *                           the key was absent in the map before current {@code ChronicleMap}
     * @param hasValueChanged    {@code true} if the {@code newValue } equals {@code replacedValue
     *                           }
     * @param identifier         the identifer used for replicaiton or zero if not definded
     * @param replacedIdentifier the replaced identifer used for replicaiton or zero if not
     *                           definded
     * @param timeStamp          the  timestamp used for replicaiton or zero if not definded
     * @param replacedTimeStamp  the replaced timestamp used for replicaiton or zero if not
     *                           definded
     */
    public void onPut(K key,
                      V newValue,
                      @Nullable V replacedValue,
                      boolean replicationEvent,
                      boolean added,
                      boolean hasValueChanged,
                      byte identifier,
                      byte replacedIdentifier, long timeStamp, long replacedTimeStamp) {

    }


    private static class LoggingMapEventListener extends MapEventListener {
        private static final long serialVersionUID = 0L;
        public final static Logger LOGGER = LoggerFactory.getLogger(LoggingMapEventListener.class);
        private final String prefix;

        private LoggingMapEventListener(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public void onGetFound(Object key, Object value) {
            LOGGER.info("get {} => {}", prefix, key, value);
        }

        @Override
        public void onPut(Object key, Object value, Object replacedValue, boolean
                replicationEvent, boolean updateResult, boolean hasValueChanged, byte identifier,
                          byte replacedIdentifier, long timeStamp, long replacedTimeStamp) {
            LOGGER.info("{} put {} => {}", prefix, key, value);
        }

        @Override
        public void onRemove(Object key, Object value, boolean replicationEvent, byte identifier, byte replacedIdentifier, long timestamp, long replacedTimeStamp) {
            LOGGER.info("{} remove {} was {}", prefix, key, value);
        }
    }

    /**
     * This method is called if the key is found in the map during {@link ChronicleMap#get get},
     * {@link ChronicleMap#getUsing getUsing} or {@link ChronicleMap#acquireUsing acquireUsing}
     * method call.
     *
     * @param key        the key looked up
     * @param foundValue the value found for the key
     */
    public void onGetFound(K key, V foundValue) {
        // do nothing
    }

    /**
     * This is called when an entry is removed. Misses, i. e. when {@code map.remove(key)} is
     * called, but key is already absent in the map, are not notified.
     *
     * @param key                the key removed from the map
     * @param value              the value which was associated with the given key
     * @param replicationEvent   {@code true} if its a replication event
     * @param identifier         the identifer used for replicaiton or zero if not definded
     * @param replacedIdentifier the replaced identifer used for replicaiton or zero if not
     *                           definded
     * @param timestamp          the  timestamp used for replicaiton or zero if not definded
     * @param replacedTimeStamp  the replaced timestamp used for replicaiton or zero if not
     *                           definded
     */
    public void onRemove(K key, V value, boolean replicationEvent, byte identifier,
                         byte replacedIdentifier, long timestamp, long replacedTimeStamp) {
        // do nothing
    }
}

