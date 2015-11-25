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
import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;

import static net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded;

/**
 * Marshaller of {@code Set<T>}. Uses {@link HashSet} (hence default element objects' equality and
 * {@code hashCode()}) as the set implementation to deserialize into.
 *
 * <p>This marshaller supports multimap emulation on top of Chronicle Map, that is possible but
 * inefficient. See <a href="https://github.com/OpenHFT/Chronicle-Map#chronicle-map-is-not">the
 * README section</a>.
 *
 * <p>Usage: <pre>{@code
 * SetMarshaller<String> valueMarshaller = new SetMarshaller<>(
 *     new StringBytesReader(), CharSequenceBytesWriter.INSTANCE);
 * ChronicleMap<String, Set<String>> map = ChronicleMap
 *     .of(String.class, (Class<Set<String>>) (Class) Set.class)
 *     .entries(10_000)
 *     .averageKey("fruits")
 *     .valueMarshaller(valueMarshaller)
 *     .averageValue(ImmutableSet.of("apples", "bananas", "grapes"))
 *     .create();
 * }</pre>
 *
 * <p>Look for pre-defined element marshallers in {@link
 * net.openhft.chronicle.hash.serialization.impl} package. This package is not included into
 * Javadocs, but present in Chronicle Map distribution. If there are no existing marshallers for
 * your {@code Set} element type, define {@link BytesReader} and {@link BytesWriter} yourself.
 *
 * @param <T> the element type of serialized Sets
 * @see ListMarshaller
 * @see MapMarshaller
 */
public class SetMarshaller<T>
        implements BytesReader<Set<T>>, BytesWriter<Set<T>>, StatefulCopyable<SetMarshaller<T>> {

    // Config fields
    private BytesReader<T> elementReader;
    private BytesWriter<? super T> elementWriter;

    /** Cache field */
    private transient Deque<T> orderedElements;

    public SetMarshaller(BytesReader<T> elementReader, BytesWriter<? super T> elementWriter) {
        this.elementReader = elementReader;
        this.elementWriter = elementWriter;
        initTransients();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        initTransients();
    }

    private void initTransients() {
        orderedElements = new ArrayDeque<>();
    }

    @NotNull
    @Override
    public Set<T> read(Bytes in, @Nullable Set<T> using) {
        int size = in.readInt();
        if (using == null) {
            using = new HashSet<>((int) (size / 0.75));
            for (int i = 0; i < size; i++) {
                using.add(elementReader.read(in, null));
            }
        } else {
            orderedElements.addAll(using);
            using.clear();
            for (int i = 0; i < size; i++) {
                using.add(elementReader.read(in, orderedElements.pollFirst()));
            }
            orderedElements.clear(); // for GC, avoid zombie object links
        }
        return using;
    }

    @Override
    public void write(Bytes out, @NotNull Set<T> toWrite) {
        out.writeInt(toWrite.size());
        toWrite.forEach(e -> elementWriter.write(out, e));
    }

    @Override
    public SetMarshaller<T> copy() {
        return new SetMarshaller<>(copyIfNeeded(elementReader), copyIfNeeded(elementWriter));
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) throws IORuntimeException {
        elementReader = wireIn.read(() -> "elementReader").typedMarshallable();
        elementWriter = wireIn.read(() -> "elementWriter").typedMarshallable();
        initTransients();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        wireOut.write(() -> "elementReader").typedMarshallable(elementReader);
        wireOut.write(() -> "elementWriter").typedMarshallable(elementWriter);
    }
}
