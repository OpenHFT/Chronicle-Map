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
import net.openhft.chronicle.hash.ChronicleHashBuilder;
import net.openhft.chronicle.hash.serialization.impl.ConstantSizeMarshaller;
import net.openhft.chronicle.hash.serialization.impl.StopBitSizeMarshaller;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.wire.Marshallable;

/**
 * Strategy of storing numbers in bytes stores (usually sizes of the subsequent chunks in
 * the same bytes store). Normal number storing (4 bytes for {@code int}, 8 bytes for {@code long}
 * is rather wasteful for marshalling purposes, when the number (size) to store is usually very
 * small, and 1-2 bytes could be enough to encode it (this is how {@link #stopBit()} marshaller
 * works). Also, this interface allows to generalize storing constantly-sized and variable-sized
 * blocks of data. Constantly-sized don't require to store the size actually, the corresponding
 * {@link #constant} {@code SizeMarshaller} consumes 0 bytes.
 * <p>
 * <p>Some {@code SizeMarshaller} implementations couldn't store the whole {@code long} range, for
 * example each {@link #constant} {@code SizeMarshaller} is able to "store" only a single specific
 * value (it's constant size). If the marshaller is fed with the size it is not able store, it could
 * throw an {@code IllegalArgumentException}.
 *
 * @see ChronicleHashBuilder#keySizeMarshaller(SizeMarshaller)
 * @see ChronicleMapBuilder#valueSizeMarshaller(SizeMarshaller)
 */
public interface SizeMarshaller extends Marshallable {

    /**
     * Returns a {@code SizeMarshaller} which stores number using so-called <a
     * href="https://github.com/OpenHFT/RFC/blob/master/Stop-Bit-Encoding/Stop-Bit-Encoding-0.1.md#signed-integers"
     * >stop bit encoding</a>. This marshaller is used as the default marshaller for keys' and
     * values' sizes in Chronicle Map, unless Chronicle Map could figure out (or hinted in {@code
     * ChronicleMapBuilder}) that keys or values are constant-sized, in which case {@link
     * #constant(long)} {@code SizeMarshaller} is used.
     */
    static SizeMarshaller stopBit() {
        return StopBitSizeMarshaller.INSTANCE;
    }

    /**
     * Returns a {@code SizeMarshaller}, that consumes 0 bytes and is above to "store" only a single
     * constant number, provided to this factory method. This size marshaller is used in Chronicle
     * Map to serialize keys' or values' sizes, when Chronicle Map could figure out (or was hinted
     * in {@code ChronicleMapBuilder}) that keys or values are constant-sized.
     *
     * @param constantSize the single constant size the returned {@code SizeMarshaller} is able to
     *                     "store"
     * @return a {@code SizeMarshaller}, that is able to "store" only the given constant size
     */
    static SizeMarshaller constant(long constantSize) {
        return new ConstantSizeMarshaller(constantSize);
    }

    /**
     * Calculates how many bytes is takes to store the given {@code size} with this {@code
     * SizeMarshaller}.
     *
     * @param size the size to store
     * @return the number of bytes would be taken to store the given size
     * @throws IllegalArgumentException might be thrown, if the given size if not storable by this
     *                                  {@code SizeMarshaller}
     */
    int storingLength(long size);

    /**
     * Returns the minimum size this {@code SizeMarshaller} is able to store.
     *
     * @return the minimum size this {@code SizeMarshaller} is able to store
     */
    long minStorableSize();

    /**
     * Returns the maximum size this {@code SizeMarshaller} is able to store.
     *
     * @return the maximum size this {@code SizeMarshaller} is able to store
     */
    long maxStorableSize();

    /**
     * Returns the minimum storing length this {@code SizeMarshaller} produces for some size in the
     * given range (both bounds are inclusive).
     *
     * @param minSize the lower bound of the range (inclusive)
     * @param maxSize the upper bound of the range (inclusive)
     * @return the smallest storing length of a value in the given range
     * @see #maxStoringLengthOfSizesInRange(long, long)
     */
    int minStoringLengthOfSizesInRange(long minSize, long maxSize);

    /**
     * Returns the maximum storing length this {@code SizeMarshaller} produces for some value in the
     * given range (both bounds are inclusive).
     *
     * @param minSize the lower bound of the range (inclusive)
     * @param maxSize the upper bound of the range (inclusive)
     * @return the biggest storing length of a value in the given range
     * @see #minStoringLengthOfSizesInRange(long, long)
     */
    int maxStoringLengthOfSizesInRange(long minSize, long maxSize);

    /**
     * Writes the given size into the streaming output.
     *
     * @param out         the {@code StreamingDataOutput} to write the size to
     * @param sizeToWrite the size to write
     * @throws IllegalArgumentException might be thrown, if the given size if not storable by this
     *                                  {@code SizeMarshaller}
     * @see #readSize(Bytes)
     */
    void writeSize(Bytes out, long sizeToWrite);

    /**
     * Reads and returns a size from the input.
     *
     * @param in the {@code StreamingDataInput} to read the size from
     * @return the read size
     * @see #writeSize(Bytes, long)
     */
    long readSize(Bytes in);
}
