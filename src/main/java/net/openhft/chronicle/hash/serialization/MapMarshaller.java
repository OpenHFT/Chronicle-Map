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

package net.openhft.chronicle.hash.serialization;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import static net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded;

/**
 * Marshaller of {@code Map<K, V>}. Uses {@link HashMap} (hence default key objects' equality and
 * {@code hashCode()} as the map implementation to deserialize into.
 * <p>
 * <p>This marshaller supports multimap emulation on top of Chronicle Map, that is possible but
 * inefficient. See <a href="https://github.com/OpenHFT/Chronicle-Map#chronicle-map-is-not">the
 * README section</a>.
 * <p>
 * <p>Look for pre-defined key and value marshallers in {@link
 * net.openhft.chronicle.hash.serialization.impl} package. This package is not included into
 * Javadocs, but present in Chronicle Map distribution. If there are no existing marshallers for
 * your {@code Map} key or value types, define {@link BytesReader} and {@link BytesWriter} yourself.
 *
 * @param <K> the key type of serialized Maps
 * @param <V> the value type of serialized Maps
 * @see ListMarshaller
 * @see SetMarshaller
 */
public final class MapMarshaller<K, V> implements BytesReader<Map<K, V>>, BytesWriter<Map<K, V>>,
        StatefulCopyable<MapMarshaller<K, V>> {

    // Config fields
    private BytesReader<K> keyReader;
    private BytesWriter<? super K> keyWriter;
    private BytesReader<V> valueReader;
    private BytesWriter<? super V> valueWriter;

    // Cache fields
    private transient Deque<K> orderedKeys;
    private transient Deque<V> orderedValues;

    /**
     * Constructs a {@code MapMarshaller} with the given map keys' and values' serializers.
     *
     * @param keyReader   map keys' reader
     * @param keyWriter   map keys' writer
     * @param valueReader map values' reader
     * @param valueWriter map values' writer
     */
    public MapMarshaller(
            BytesReader<K> keyReader, BytesWriter<? super K> keyWriter,
            BytesReader<V> valueReader, BytesWriter<? super V> valueWriter) {
        this.keyReader = keyReader;
        this.keyWriter = keyWriter;
        this.valueReader = valueReader;
        this.valueWriter = valueWriter;
        initTransients();
    }

    private void initTransients() {
        orderedKeys = new ArrayDeque<>();
        orderedValues = new ArrayDeque<>();
    }

    @NotNull
    @Override
    public Map<K, V> read(Bytes in, @Nullable Map<K, V> using) {
        int size = in.readInt();
        if (using == null) {
            using = new HashMap<>(((int) (size / 0.75)));
            for (int i = 0; i < size; i++) {
                using.put(keyReader.read(in, null), valueReader.read(in, null));
            }
        } else {
            using.forEach((k, v) -> {
                orderedKeys.add(k);
                orderedValues.add(v);
            });
            using.clear();
            for (int i = 0; i < size; i++) {
                using.put(keyReader.read(in, orderedKeys.pollFirst()),
                        valueReader.read(in, orderedValues.pollFirst()));
            }
            orderedKeys.clear(); // for GC, avoid zombie object links
            orderedValues.clear();
        }
        return using;
    }

    @Override
    public void write(Bytes out, @NotNull Map<K, V> toWrite) {
        out.writeInt(toWrite.size());
        toWrite.forEach((k, v) -> {
            keyWriter.write(out, k);
            valueWriter.write(out, v);
        });
    }

    @Override
    public MapMarshaller<K, V> copy() {
        return new MapMarshaller<>(copyIfNeeded(keyReader), copyIfNeeded(keyWriter),
                copyIfNeeded(valueReader), copyIfNeeded(valueWriter));
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        keyReader = wireIn.read(() -> "keyReader").typedMarshallable();
        keyWriter = wireIn.read(() -> "keyWriter").typedMarshallable();
        valueReader = wireIn.read(() -> "valueReader").typedMarshallable();
        valueWriter = wireIn.read(() -> "valueWriter").typedMarshallable();
        initTransients();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        wireOut.write(() -> "keyReader").typedMarshallable(keyReader);
        wireOut.write(() -> "keyWriter").typedMarshallable(keyWriter);
        wireOut.write(() -> "valueReader").typedMarshallable(valueReader);
        wireOut.write(() -> "valueWriter").typedMarshallable(valueWriter);
    }
}
