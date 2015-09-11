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

package net.openhft.chronicle.hash.hashing;

import net.openhft.lang.io.Bytes;

import static net.openhft.chronicle.hash.hashing.UnsafeAccess.BYTE_BASE;
import static net.openhft.chronicle.hash.hashing.UnsafeAccess.CHAR_BASE;

public final class Hasher {

    // MurmurHash3, derived from
    // https://github.com/google/guava/blob/fa95e381e665d8ee9639543b99ed38020c8de5ef
    // /guava/src/com/google/common/hash/Murmur3_128HashFunction.java

    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;

    private Hasher() {
    }

    /**
     * Returns the hash code for {@code len} continuous bytes of the given {@code input} object,
     * starting from the given offset. The abstraction of input as ordered byte sequence and
     * "offset within the input" is defined by the given {@code access} strategy.
     *
     * <p>This method doesn't promise to throw a {@code RuntimeException} if {@code
     * [off, off + len - 1]} subsequence exceeds the bounds of the bytes sequence, defined by {@code
     * access} strategy for the given {@code input}, so use this method with caution.
     *
     * @param input  the object to read bytes from
     * @param access access which defines the abstraction of the given input
     *               as ordered byte sequence
     * @param offset offset to the first byte of the subsequence to hash
     * @param length length of the subsequence to hash
     * @param <T> the type of the input
     * @return hash code for the specified bytes subsequence
     */
    public static <T> long hash(T input, Access<T> access, long offset, long length) {
        assert offset >= 0L && length >= 0L;

        long h1 = 0L;
        long h2 = 0L;
        long remaining = length;
        while (remaining >= 16L) {
            long k1 = access.getLong(input, offset);
            long k2 = access.getLong(input, offset + 8L);
            offset += 16L;
            remaining -= 16L;
            h1 ^= mixK1(k1);

            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5L + 0x52dce729L;

            h2 ^= mixK2(k2);

            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5L + 0x38495ab5L;
        }

        if (remaining > 0L) {
            long k1 = 0L;
            long k2 = 0L;
            switch ((int) remaining) {
                case 15:
                    k2 ^= ((long) access.getUnsignedByte(input, offset + 14L)) << 48;// fall through
                case 14:
                    k2 ^= ((long) access.getUnsignedByte(input, offset + 13L)) << 40;// fall through
                case 13:
                    k2 ^= ((long) access.getUnsignedByte(input, offset + 12L)) << 32;// fall through
                case 12:
                    k2 ^= ((long) access.getUnsignedByte(input, offset + 11L)) << 24;// fall through
                case 11:
                    k2 ^= ((long) access.getUnsignedByte(input, offset + 10L)) << 16;// fall through
                case 10:
                    k2 ^= ((long) access.getUnsignedByte(input, offset + 9L)) << 8; // fall through
                case 9:
                    k2 ^= ((long) access.getUnsignedByte(input, offset + 8L)); // fall through
                case 8:
                    k1 ^= access.getLong(input, offset);
                    break;
                case 7:
                    k1 ^= ((long) access.getUnsignedByte(input, offset + 6L)) << 48; // fall through
                case 6:
                    k1 ^= ((long) access.getUnsignedByte(input, offset + 5L)) << 40; // fall through
                case 5:
                    k1 ^= ((long) access.getUnsignedByte(input, offset + 4L)) << 32; // fall through
                case 4:
                    k1 ^= access.getUnsignedInt(input, offset);
                    break;
                case 3:
                    k1 ^= ((long) access.getUnsignedByte(input, offset + 2L)) << 16; // fall through
                case 2:
                    k1 ^= ((long) access.getUnsignedByte(input, offset + 1L)) << 8; // fall through
                case 1:
                    k1 ^= ((long) access.getUnsignedByte(input, offset));
                case 0:
                    break;
                default:
                    throw new AssertionError("Should never get here.");
            }
            h1 ^= mixK1(k1);
            h2 ^= mixK2(k2);
        }

        return finalize(length, h1, h2);
    }

    private static long finalize(long length, long h1, long h2) {
        h1 ^= length;
        h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = fmix64(h1);
        h2 = fmix64(h2);

        h1 += h2;
        return h1;
    }

    private static long fmix64(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    private static long mixK1(long k1) {
        k1 *= C1;
        k1 = Long.rotateLeft(k1, 31);
        k1 *= C2;
        return k1;
    }

    private static long mixK2(long k2) {
        k2 *= C2;
        k2 = Long.rotateLeft(k2, 33);
        k2 *= C1;
        return k2;
    }

    public static long hash(Bytes bytes) {
        return hash(bytes, bytes.position(), bytes.limit());
    }

    public static long hash(Bytes bytes, long offset, long limit) {
        return hash(bytes, Accesses.toBytes(), offset, limit - offset);
    }

    public static long hash(byte[] array) {
        return hash(array, Accesses.unsafe(), BYTE_BASE, (long) array.length);
    }

    public static long hash(char[] array) {
        return hash(array, Accesses.unsafe(), CHAR_BASE, array.length * 2L);
    }

    public static long hash(int value) {
        long k1 = Primitives.unsignedInt(value);
        long h1 = 0L ^ mixK1(k1);
        long h2 = 0L;
        return finalize(4L, h1, h2);
    }

    public static long hash(long value) {
        long k1 = value;
        long h1 = 0L ^ mixK1(k1);
        long h2 = 0L;
        return finalize(8L, h1, h2);
    }
}
