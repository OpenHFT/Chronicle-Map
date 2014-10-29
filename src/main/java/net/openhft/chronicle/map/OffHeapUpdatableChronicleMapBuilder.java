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

package net.openhft.chronicle.map;

import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;
import org.jetbrains.annotations.NotNull;

public final class OffHeapUpdatableChronicleMapBuilder<K, V>
        extends AbstractChronicleMapBuilder<K, V, OffHeapUpdatableChronicleMapBuilder<K, V>> {

    public static <K, V> OffHeapUpdatableChronicleMapBuilder<K, V> of(
            @NotNull Class<K> keyClass, @NotNull Class<V> valueClass) {
        if (!Byteable.class.isAssignableFrom(valueClass)) {
            if (!valueClass.isInterface()) {
                throw new IllegalArgumentException(
                        "Value class should be either Byteable subclass or interface," +
                                "allowing direct Byteable implementation generation");
            }
            valueClass = DataValueClasses.directClassFor(valueClass);
        }
        return new OffHeapUpdatableChronicleMapBuilder<K, V>(keyClass, valueClass);
    }

    OffHeapUpdatableChronicleMapBuilder(Class<K> keyClass, Class<V> valueClass) {
        super(keyClass, valueClass);
        prepareValueBytesOnAcquire(new ZeroOutValueBytes<K>(valueSize()));
    }

    @Override
    OffHeapUpdatableChronicleMapBuilder<K, V> self() {
        return this;
    }

    /**
     * {@inheritDoc} With respect to {@linkplain #entryAndValueAlignment(Alignment) alignment}.
     *
     * <p>Note that the actual entrySize will be aligned to 4 (default {@linkplain
     * #entryAndValueAlignment(Alignment) entry alignment}). I. e. if you set entry size to 30,
     * and entry alignment is set to {@link Alignment#OF_4_BYTES}, the actual entry size will be 32
     * (30 aligned to 4 bytes).
     *
     * @see #entryAndValueAlignment(Alignment)
     * @see #entries(long)
     */
    @Override
    public OffHeapUpdatableChronicleMapBuilder<K, V> entrySize(int entrySize) {
        return super.entrySize(entrySize);
    }

    /**
     * Configures alignment strategy of address in memory of entries and independently of address in
     * memory of values within entries in ChronicleMaps, created by this builder.
     *
     * <p>Useful when values of the map are updated intensively, particularly fields with volatile
     * access, because it doesn't work well if the value crosses cache lines. Also, on some
     * (nowadays rare) architectures any misaligned memory access is more expensive than aligned.
     *
     * <p>Note that {@linkplain #entrySize(int) entry size} will be aligned according to this
     * alignment. I. e. if you set {@code entrySize(20)} and {@link Alignment#OF_8_BYTES}, actual
     * entry size will be 24 (20 aligned to 8 bytes).
     *
     * <p>Default is {@link Alignment#NO_ALIGNMENT}.
     *
     * @param alignment the new alignment of the maps constructed by this builder
     * @return this {@code ChronicleMapBuilder} back
     */
    @Override
    public OffHeapUpdatableChronicleMapBuilder<K, V> entryAndValueAlignment(Alignment alignment) {
        return super.entryAndValueAlignment(alignment);
    }

    /**
     * {@inheritDoc} Also, it overrides any previous {@link #prepareValueBytesOnAcquire}
     * configuration to this {@code OffHeapUpdatableChronicleMapBuilder}.
     *
     * <p>By default, the default value is not specified, default {@linkplain
     * #prepareValueBytesOnAcquire prepare value bytes routine} is specified instead.
     *
     * @see #defaultValueProvider(DefaultValueProvider)
     * @see #prepareValueBytesOnAcquire(PrepareValueBytes)
     */
    @Override
    public OffHeapUpdatableChronicleMapBuilder<K, V> defaultValue(V defaultValue) {
        return super.defaultValue(defaultValue);
    }

    /**
     * {@inheritDoc} Also, it overrides any previous {@link #prepareValueBytesOnAcquire}
     * configuration to this {@code OffHeapUpdatableChronicleMapBuilder}.
     *
     * <p>By default, the default value provider is not specified, default {@linkplain
     * #prepareValueBytesOnAcquire prepare value bytes routine} is specified instead.
     *
     * @see #defaultValue(Object)
     * @see #prepareValueBytesOnAcquire(PrepareValueBytes)
     */
    @Override
    public OffHeapUpdatableChronicleMapBuilder<K, V> defaultValueProvider(
            @NotNull DefaultValueProvider<K, V> defaultValueProvider) {
        return super.defaultValueProvider(defaultValueProvider);
    }

    /**
     * Configures the procedure which is called on the bytes, which later the returned value is
     * pointing to, if the key is absent, on {@link ChronicleMap#acquireUsing(Object, Object)
     * acquireUsing()} call on maps, created by this builder.
     *
     * <p>The default preparation callback zeroes out the value bytes.
     *
     * @param prepareValueBytes what to do with the value bytes before assigning them into the
     *                          {@link Byteable} value to return from {@code acquireUsing()} call
     * @return this builder back
     * @see #defaultValue(Object)
     * @see #defaultValueProvider(DefaultValueProvider)
     */
    @Override
    public OffHeapUpdatableChronicleMapBuilder<K, V> prepareValueBytesOnAcquire(
            @NotNull PrepareValueBytes<K> prepareValueBytes) {
        return super.prepareValueBytesOnAcquire(prepareValueBytes);
    }
}
