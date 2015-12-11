/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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

import java.util.ArrayList;
import java.util.List;

import static net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded;

/**
 * Marshaller of {@code List<T>}. Uses {@link ArrayList} as the list implementation to deserialize
 * into.
 *
 * <p>This marshaller supports multimap emulation on top of Chronicle Map, that is possible but
 * inefficient. See <a href="https://github.com/OpenHFT/Chronicle-Map#chronicle-map-is-not">the
 * README section</a>.
 *
 * <p>Usage: <pre>{@code
 * ListMarshaller<Integer> valueMarshaller = new ListMarshaller<>(
 *     IntegerMarshaller.INSTANCE, IntegerMarshaller.INSTANCE);
 * ChronicleMap<String, List<Integer>> regNumbers = ChronicleMap
 *     .of(String.class, (Class<List<Integer>>) (Class) List.class)
 *     .entries(10_000)
 *     .averageKey("John Smith")
 *     .valueMarshaller(valueMarshaller)
 *     .averageValue(ImmutableList.of(1, 2, 3))
 *     .create();
 * }</pre>
 *
 * <p>Look for pre-defined element marshallers in {@link
 * net.openhft.chronicle.hash.serialization.impl} package. This package is not included into
 * Javadocs, but present in Chronicle Map distribution. If there are no existing marshallers for
 * your {@code List} element type, define {@link BytesReader} and {@link BytesWriter} yourself.
 *
 * @param <T> the element type of serialized Lists
 * @see SetMarshaller
 * @see MapMarshaller
 */
public final class ListMarshaller<T>
        implements BytesReader<List<T>>, BytesWriter<List<T>>, StatefulCopyable<ListMarshaller<T>> {

    // Config fields
    private BytesReader<T> elementReader;
    private BytesWriter<? super T> elementWriter;

    public ListMarshaller(BytesReader<T> elementReader, BytesWriter<? super T> elementWriter) {
        this.elementReader = elementReader;
        this.elementWriter = elementWriter;
    }

    @NotNull
    @Override
    public List<T> read(Bytes in, @Nullable List<T> using) {
        int size = in.readInt();
        if (using == null) {
            using = new ArrayList<>(size);
        } else if (using.size() < size) {
            while (using.size() < size) {
                using.add(null);
            }
        } else if (using.size() > size) {
            using.subList(size, using.size()).clear();
        }
        for (int i = 0; i < size; i++) {
            using.set(i, elementReader.read(in, using.get(i)));
        }
        return using;
    }

    @Override
    public void write(Bytes out, @NotNull List<T> toWrite) {
        out.writeInt(toWrite.size());
        // indexed loop to avoid garbage creation
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < toWrite.size(); i++) {
            elementWriter.write(out, toWrite.get(i));
        }
    }

    @Override
    public ListMarshaller<T> copy() {
        if (elementReader instanceof StatefulCopyable ||
                elementWriter instanceof StatefulCopyable) {
            return new ListMarshaller<>(copyIfNeeded(elementReader), copyIfNeeded(elementWriter));
        } else {
            return this;
        }
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        elementReader = wireIn.read(() -> "elementReader").typedMarshallable();
        elementWriter = wireIn.read(() -> "elementWriter").typedMarshallable();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        wireOut.write(() -> "elementReader").typedMarshallable(elementReader);
        wireOut.write(() -> "elementWriter").typedMarshallable(elementWriter);
    }
}
