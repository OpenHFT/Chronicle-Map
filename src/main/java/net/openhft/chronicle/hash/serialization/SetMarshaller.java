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

package net.openhft.chronicle.hash.serialization;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

import static net.openhft.chronicle.hash.serialization.StatefulCopyable.copyIfNeeded;

/**
 * Marshaller of {@code Set<T>}. Uses {@link HashSet} (hence default element objects' equality and
 * {@code hashCode()}) as the set implementation to deserialize into.
 * <p>
 * <p>This marshaller supports multimap emulation on top of Chronicle Map, that is possible but
 * inefficient. See <a href="https://github.com/OpenHFT/Chronicle-Map#chronicle-map-is-not">the
 * README section</a>.
 * <p>
 * <p>Usage: <pre>{@code
 * SetMarshaller<String> valueMarshaller = SetMarshaller.of(
 *     new StringBytesReader(), CharSequenceBytesWriter.INSTANCE);
 * ChronicleMap<String, Set<String>> map = ChronicleMap
 *     .of(String.class, (Class<Set<String>>) (Class) Set.class)
 *     .averageKey("fruits")
 *     .valueMarshaller(valueMarshaller)
 *     .averageValue(ImmutableSet.of("apples", "bananas", "grapes"))
 *     .entries(10_000)
 *     .create();
 * }</pre>
 * <p>
 * <p>Look for pre-defined element marshallers in {@link
 * net.openhft.chronicle.hash.serialization.impl} package. This package is not included into
 * Javadocs, but present in Chronicle Map distribution. If there are no existing marshallers for
 * your {@code Set} element type, define {@link BytesReader} and {@link BytesWriter} yourself.
 *
 * @param <T> the element type of serialized Sets
 * @see ListMarshaller
 * @see MapMarshaller
 */
public final class SetMarshaller<T>
        implements BytesReader<Set<T>>, BytesWriter<Set<T>>, StatefulCopyable<SetMarshaller<T>> {

    // Config fields
    private BytesReader<T> elementReader;
    private BytesWriter<? super T> elementWriter;
    /**
     * Cache field
     */
    private transient Deque<T> orderedElements;
    /**
     * Constructs a {@code SetMarshaller} with the given set elements' serializers.
     * <p>
     * <p>Use static factory {@link #of(BytesReader, BytesWriter)} instead of this constructor
     * directly.
     *
     * @param elementReader set elements' reader
     * @param elementWriter set elements' writer
     */
    public SetMarshaller(BytesReader<T> elementReader, BytesWriter<? super T> elementWriter) {
        this.elementReader = elementReader;
        this.elementWriter = elementWriter;
        initTransients();
    }

    /**
     * Returns a {@code SetMarshaller} which uses the given set elements' serializers.
     *
     * @param elementReader set elements' reader
     * @param elementWriter set elements' writer
     * @param <T>           type of set elements
     * @return a {@code SetMarshaller} which uses the given set elements' serializers
     */
    public static <T> SetMarshaller<T> of(
            BytesReader<T> elementReader, BytesWriter<? super T> elementWriter) {
        return new SetMarshaller<>(elementReader, elementWriter);
    }

    /**
     * Returns a {@code SetMarshaller} which uses the given marshaller as both reader and writer of
     * set elements. Example: <pre><code>
     * ChronicleMap
     *     .of(String.class,{@code (Class<Set<Integer>>)} ((Class) Set.class))
     *     .valueMarshaller(SetMarshaller.of(IntegerMarshaller.INSTANCE))
     *     ...</code></pre>
     *
     * @param elementMarshaller set elements' marshaller
     * @param <T>               type of set elements
     * @param <M>               type of set elements' marshaller
     * @return a {@code SetMarshaller} which uses the given set elements' marshaller
     */
    public static <T, M extends BytesReader<T> & BytesWriter<? super T>> SetMarshaller<T> of(
            M elementMarshaller) {
        return of(elementMarshaller, elementMarshaller);
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
    public void readMarshallable(@NotNull WireIn wireIn) {
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
