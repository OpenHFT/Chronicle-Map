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

package net.openhft.chronicle.hash.hashing;

import net.openhft.chronicle.hash.serialization.BytesInterop;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.NativeBytes;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.ByteOrder.nativeOrder;
import static net.openhft.chronicle.hash.hashing.CharSequenceAccess.nativeCharSequenceAccess;
import static net.openhft.chronicle.hash.hashing.UnsafeAccess.*;

/**
 * Hash function producing {@code long}-valued result from byte sequences of any length and
 * a plenty of different sources which "feels like byte sequences". Except {@link
 * #hashBytes(byte[])}, {@link #hashBytes(ByteBuffer)} (with their "sliced" versions) and
 * {@link #hashMemory(long, long)} methods, which actually accept byte sequences, notion of byte
 * sequence is defined as follows:
 * <ul>
 *     <li>For methods accepting arrays of Java primitives, {@code String}s and
 *     {@code StringBuilder}s, byte sequence is how the input's bytes are actually lay in memory.
 *     </li>
 *     <li>For methods accepting single primitive values, byte sequence is how this primitive
 *     would be put into memory with {@link ByteOrder#nativeOrder() native} byte order, or
 *     equivalently, {@code hashXxx(primitive)} has always the same result as {@code
 *     hashXxxs(new xxx[] {primitive})}, where "xxx" is any Java primitive type name.</li>
 *     <li>For {@link #hash(Object, Access, long, long)} method byte sequence abstraction
 *     is defined by the given {@link Access} strategy to the given object.</li>
 * </ul>
 * 
 * @see BytesInterop#hash(LongHashFunction, Object)
 * @deprecated moved to Chronicle-Algorithms:
 * {@link net.openhft.chronicle.algo.hashing.LongHashFunction} 
 */
@Deprecated
public abstract class LongHashFunction implements Serializable {
    private static final long serialVersionUID = 0L;

    static final boolean NATIVE_LITTLE_ENDIAN = nativeOrder() == LITTLE_ENDIAN;
    static final byte TRUE_BYTE_VALUE = UNSAFE.getByte(new boolean[] {true}, BOOLEAN_BASE);
    static final byte FALSE_BYTE_VALUE = UNSAFE.getByte(new boolean[] {false}, BOOLEAN_BASE);

    /**
     * Returns a hash function implementing
     * <a href="https://code.google.com/p/cityhash/source/browse/trunk/src/city.cc?r=10">
     * CityHash64 algorithm, version 1.1</a> without seed values. This implementation produce
     * equal results for equal input on platforms with different {@link ByteOrder}, but is slower
     * on big-endian platforms than on little-endian.
     * @return an instance of LongHashFunction
     *
     * @see #city_1_1(long)
     * @see #city_1_1(long, long)
     */
    public static LongHashFunction city_1_1() {
        return CityHash_1_1.asLongHashFunctionWithoutSeed();
    }

    /**
     * Returns a hash function implementing
     * <a href="https://code.google.com/p/cityhash/source/browse/trunk/src/city.cc?r=10">
     * CityHash64 algorithm, version 1.1</a> using the given seed value. This implementation produce
     * equal results for equal input on platforms with different {@link ByteOrder}, but is slower
     * on big-endian platforms than on little-endian.
     *
     * @see #city_1_1()
     * @see #city_1_1(long, long)
     */
    public static LongHashFunction city_1_1(long seed) {
        return CityHash_1_1.asLongHashFunctionWithSeed(seed);
    }

    /**
     * Returns a hash function implementing
     * <a href="https://code.google.com/p/cityhash/source/browse/trunk/src/city.cc?r=10">
     * CityHash64 algorithm, version 1.1</a> using the two given seed values. This implementation
     * produce equal results for equal input on platforms with different {@link ByteOrder}, but
     * is slower on big-endian platforms than on little-endian.
     * @return an instance of LongHashFunction
     * @see #city_1_1()
     * @see #city_1_1(long)
     */
    public static LongHashFunction city_1_1(long seed0, long seed1) {
        return CityHash_1_1.asLongHashFunctionWithTwoSeeds(seed0, seed1);
    }

    private static StringHash stringHash;
    static  {
        try {
            if (System.getProperty("java.vm.name").contains("HotSpot")) {
                if (System.getProperty("java.version").compareTo("1.7.0_06") >= 0) {
                    stringHash = ModernHotSpotStringHash.INSTANCE;
                } else {
                    stringHash = HotSpotPrior7u6StringHash.INSTANCE;
                }
            } else {
                // try to initialize this version anyway
                stringHash = HotSpotPrior7u6StringHash.INSTANCE;
            }
        } catch (Throwable e) {
            // ignore
        } finally {
            if (stringHash == null)
                stringHash = UnknownJvmStringHash.INSTANCE;
        }
    }

    private static void checkArrayOffs(int arrayLength, int off, int len) {
        if (len < 0 || off < 0 || off + len > arrayLength || off + len < 0)
            throw new IndexOutOfBoundsException();
    }

    /**
     * Constructor for use in subclasses.
     */
    protected LongHashFunction() {}

    /**
     * Returns the hash code for the given {@code long} value; this method is consistent with
     * {@code LongHashFunction} methods that accept sequences of bytes, assuming the {@code input}
     * value is interpreted in {@linkplain ByteOrder#nativeOrder() native} byte order. For example,
     * the result of {@code hashLong(v)} call is identical to the result of
     * {@code hashLongs(new long[] {v})} call for any {@code long} value.
     * @param input the input
     * @return a hashLong
     */
    public abstract long hashLong(long input);

    /**
     * Returns the hash code for the given {@code int} value; this method is consistent with
     * {@code LongHashFunction} methods that accept sequences of bytes, assuming the {@code input}
     * value is interpreted in {@linkplain ByteOrder#nativeOrder() native} byte order. For example,
     * the result of {@code hashInt(v)} call is identical to the result of
     * {@code hashInts(new int[] {v})} call for any {@code int} value.
           * @param input the input
     * @return a hashLong
     */
    public abstract long hashInt(int input);

    /**
     * Returns the hash code for the given {@code short} value; this method is consistent with
     * {@code LongHashFunction} methods that accept sequences of bytes, assuming the {@code input}
     * value is interpreted in {@linkplain ByteOrder#nativeOrder() native} byte order. For example,
     * the result of {@code hashShort(v)} call is identical to the result of
     * {@code hashShorts(new short[] {v})} call for any {@code short} value.
     * As a consequence, {@code hashShort(v)} call produce always the same result as {@code
     * hashChar((char) v)}.
     *
     * @param input the input
     * @return a hashShort
     */
    public abstract long hashShort(short input);

    /**
     * Returns the hash code for the given {@code char} value; this method is consistent with
     * {@code LongHashFunction} methods that accept sequences of bytes, assuming the {@code input}
     * value is interpreted in {@linkplain ByteOrder#nativeOrder() native} byte order. For example,
     * the result of {@code hashChar(v)} call is identical to the result of
     * {@code hashChars(new char[] {v})} call for any {@code char} value.
     * As a consequence, {@code hashChar(v)} call produce always the same result as {@code
     * hashShort((short) v)}.
     */
    public abstract long hashChar(char input);

    /**
     * Returns the hash code for the given {@code byte} value. This method is consistent with
     * {@code LongHashFunction} methods that accept sequences of bytes. For example, the result of
     * {@code hashByte(v)} call is identical to the result of
     * {@code hashBytes(new byte[] {v})} call for any {@code byte} value.
     *
     * @param input the input
     * @return a hashByte
     */
    public abstract long hashByte(byte input);

    /**
     * Returns the hash code for the empty (zero-length) bytes sequence,
     * for example {@code hashBytes(new byte[0])}.
     * @return a hash
     */
    public abstract long hashVoid();

    /**
     * Returns the hash code for {@code len} continuous bytes of the given {@code input} object,
     * starting from the given offset. The abstraction of input as ordered byte sequence and
     * "offset within the input" is defined by the given {@code access} strategy.
     *
     * <p>This method doesn't promise to throw a {@code RuntimeException} if {@code
     * [off, off + len - 1]} subsequence exceeds the bounds of the bytes sequence, defined by {@code
     * access} strategy for the given {@code input}, so use this method with caution.
     *
     * @param input the object to read bytes from
     * @param access access which defines the abstraction of the given input
     *               as ordered byte sequence
     * @param off offset to the first byte of the subsequence to hash
     * @param len length of the subsequence to hash
     * @param <T> the type of the input
     * @return hash code for the specified bytes subsequence
     */
    public abstract <T> long hash(T input, Access<T> access, long off, long len);

    private long unsafeHash(Object input, long off, long len) {
        return hash(input, UnsafeAccess.INSTANCE, off, len);
    }

    /**
     * Shortcut for {@link #hashBooleans(boolean[]) hashBooleans(new boolean[] &#123;input&#125;)}.
     * Note that this is not necessarily equal to {@code hashByte(input ? (byte) 1 : (byte) 0)},
     * because booleans could be stored differently in this JVM.
     */
    public long hashBoolean(boolean input) {
        return hashByte(input ? TRUE_BYTE_VALUE : FALSE_BYTE_VALUE);
    }

    /**
     * Shortcut for {@link #hashBooleans(boolean[], int, int) hashBooleans(input, 0, input.length)}.
     */
    public long hashBooleans(@NotNull boolean[] input) {
        return hashBooleans(input, 0, input.length);
    }

    /**
     * Returns the hash code for the specified subsequence of the given {@code boolean} array.
     *
     * @param input the array to read data from
     * @param off index of the first {@code boolean} in the subsequence to hash
     * @param len length of the subsequence to hash
     * @return hash code for the specified subsequence
     * @throws IndexOutOfBoundsException if {@code off < 0} or {@code off + len > input.length}
     * or {@code len < 0}
     */
    public long hashBooleans(@NotNull boolean[] input, int off, int len) {
        checkArrayOffs(input.length, off, len);
        return unsafeHash(input, BOOLEAN_BASE + off, len);
    }

    /**
     * Shortcut for {@link #hashBytes(byte[], int, int) hashBytes(input, 0, input.length)}.
     */
    public long hashBytes(@NotNull byte[] input) {
        return hashBytes(input, 0, input.length);
    }

    /**
     * Returns the hash code for the specified subsequence of the given {@code byte} array.
     *
     * @param input the array to read bytes from
     * @param off index of the first {@code byte} in the subsequence to hash
     * @param len length of the subsequence to hash
     * @return hash code for the specified subsequence
     * @throws IndexOutOfBoundsException if {@code off < 0} or {@code off + len > input.length}
     * or {@code len < 0}
     */
    public long hashBytes(@NotNull byte[] input, int off, int len) {
        checkArrayOffs(input.length, off, len);
        return unsafeHash(input, BYTE_BASE + off, len);
    }

    /**
     * Shortcut for {@link #hashBytes(ByteBuffer, int, int)
     * hashBytes(input, input.position(), input.remaining())}.
     */
    public long hashBytes(ByteBuffer input) {
        return hashBytes(input, input.position(), input.remaining());
    }

    /**
     * Returns the hash code for the specified subsequence of the given {@code ByteBuffer}.
     *
     * <p>This method doesn't alter the state (mark, position, limit or order) of the given
     * {@code ByteBuffer}.
     *
     * @param input the buffer to read bytes from
     * @param off index of the first {@code byte} in the subsequence to hash
     * @param len length of the subsequence to hash
     * @return hash code for the specified subsequence
     * @throws IndexOutOfBoundsException if {@code off < 0} or {@code off + len > input.capacity()}
     * or {@code len < 0}
     */
    public long hashBytes(@NotNull ByteBuffer input, int off, int len) {
        checkArrayOffs(input.capacity(), off, len);
        return hash(input, ByteBufferAccess.INSTANCE, off, len);
    }

    /**
     * Shortcut for {@link #hashBytes(Bytes, long, long)
     * hashBytes(input, input.position(), input.remaining())}.
     */
    public long hashBytes(Bytes input) {
        return hashBytes(input, input.position(), input.remaining());
    }

    /**
     * Returns the hash code for the specified subsequence of the given {@code Bytes}.
     *
     * <p>This method doesn't alter the state (position or limit) of the given {@code Bytes}.
     *
     * @param input the {@code Bytes} to read bytes from
     * @param off index of the first {@code byte} in the subsequence to hash
     * @param len length of the subsequence to hash
     * @return hash code for the specified subsequence
     * @throws IndexOutOfBoundsException if {@code off < 0} or {@code off + len > input.capacity()}
     * or {@code len < 0}
     */
    public long hashBytes(Bytes input, long off, long len) {
        if (len < 0 || off < 0 || off + len > input.capacity() || off + len < 0)
            throw new IndexOutOfBoundsException();
        if (input instanceof NativeBytes) {
            return hashMemory(input.address() + off, len);
        } else {
            return hash(input, toBytes(), off, len);
        }
    }

    /**
     * Returns the hash code of bytes of the wild memory from the given address. Use with caution.
     *
     * @param address the address of the first byte to hash
     * @param len length of the byte sequence to hash
     * @return hash code for the specified byte sequence
     */
    public long hashMemory(long address, long len) {
        return unsafeHash(null, address, len);
    }

    /**
     * Shortcut for {@link #hashChars(char[], int, int) hashChars(input, 0, input.length)}.
     */
    public long hashChars(@NotNull char[] input) {
        return hashChars(input, 0, input.length);
    }

    /**
     * Returns the hash code for bytes, as they lay in memory, of the specified subsequence
     * of the given {@code char} array.
     *
     * @param input the array to read data from
     * @param off index of the first {@code char} in the subsequence to hash
     * @param len length of the subsequence to hash, in chars (i. e. the length of the bytes
     *            sequence to hash is {@code len * 2L})
     * @return hash code for the specified subsequence
     * @throws IndexOutOfBoundsException if {@code off < 0} or {@code off + len > input.length}
     * or {@code len < 0}
     */
    public long hashChars(@NotNull char[] input, int off, int len) {
        checkArrayOffs(input.length, off, len);
        return unsafeHash(input, CHAR_BASE + (off * 2L), len * 2L);
    }

    /**
     * Shortcut for {@link #hashChars(String, int, int) hashChars(input, 0, input.length())}.*
     */
    public long hashChars(@NotNull String input) {
        return stringHash.longHash(input, this, 0, input.length());
    }

    /**
     * Returns the hash code for bytes of the specified subsequence of the given {@code String}'s
     * underlying {@code char} array.
     *
     * @param input the string which bytes to hash
     * @param off index of the first {@code char} in the subsequence to hash
     * @param len length of the subsequence to hash, in chars (i. e. the length of the bytes
     *            sequence to hash is {@code len * 2L})
     * @return the hash code of the given {@code String}'s bytes
     * @throws IndexOutOfBoundsException if {@code off < 0} or {@code off + len > input.length()}
     * or {@code len < 0}
     */
    public long hashChars(@NotNull String input, int off, int len) {
        checkArrayOffs(input.length(), off, len);
        return stringHash.longHash(input, this, off, len);
    }

    /**
     * Shortcut for {@link #hashChars(StringBuilder, int, int) hashChars(input, 0, input.length())}.
     */
    public long hashChars(@NotNull StringBuilder input) {
        return hashNativeChars(input);
    }

    /**
     * Returns the hash code for bytes of the specified subsequence of the given
     * {@code StringBuilder}'s underlying {@code char} array.
     *
     * @param input the string builder which bytes to hash
     * @param off index of the first {@code char} in the subsequence to hash
     * @param len length of the subsequence to hash, in chars (i. e. the length of the bytes
     *            sequence to hash is {@code len * 2L})
     * @return the hash code of the given {@code String}'s bytes
     * @throws IndexOutOfBoundsException if {@code off < 0} or {@code off + len > input.length()}
     * or {@code len < 0}
     */
    public long hashChars(@NotNull StringBuilder input, int off, int len) {
        return hashNativeChars(input, off, len);
    }

    long hashNativeChars(CharSequence input) {
        return hashNativeChars(input, 0, input.length());
    }

    long hashNativeChars(CharSequence input, int off, int len) {
        checkArrayOffs(input.length(), off, len);
        return hash(input, nativeCharSequenceAccess(), off * 2L, len * 2L);
    }

    /**
     * Shortcut for {@link #hashShorts(short[], int, int) hashShorts(input, 0, input.length)}.
     */
    public long hashShorts(@NotNull short[] input) {
        return hashShorts(input, 0, input.length);
    }

    /**
     * Returns the hash code for bytes, as they lay in memory, of the specified subsequence
     * of the given {@code short} array.
     *
     * @param input the array to read data from
     * @param off index of the first {@code short} in the subsequence to hash
     * @param len length of the subsequence to hash, in shorts (i. e. the length of the bytes
     *            sequence to hash is {@code len * 2L})
     * @return hash code for the specified subsequence
     * @throws IndexOutOfBoundsException if {@code off < 0} or {@code off + len > input.length}
     * or {@code len < 0}
     */
    public long hashShorts(@NotNull short[] input, int off, int len) {
        checkArrayOffs(input.length, off, len);
        return unsafeHash(input, SHORT_BASE + (off * 2L), len * 2L);
    }

    /**
     * Shortcut for {@link #hashInts(int[], int, int) hashInts(input, 0, input.length)}.
     */
    public long hashInts(@NotNull int[] input) {
        return hashInts(input, 0, input.length);
    }

    /**
     * Returns the hash code for bytes, as they lay in memory, of the specified subsequence
     * of the given {@code int} array.
     *
     * @param input the array to read data from
     * @param off index of the first {@code int} in the subsequence to hash
     * @param len length of the subsequence to hash, in ints (i. e. the length of the bytes
     *            sequence to hash is {@code len * 4L})
     * @return hash code for the specified subsequence
     * @throws IndexOutOfBoundsException if {@code off < 0} or {@code off + len > input.length}
     * or {@code len < 0}
     */
    public long hashInts(@NotNull int[] input, int off, int len) {
        checkArrayOffs(input.length, off, len);
        return unsafeHash(input, INT_BASE + (off * 4L), len * 4L);
    }

    /**
     * Shortcut for {@link #hashLongs(long[], int, int) hashLongs(input, 0, input.length)}.
     */
    public long hashLongs(@NotNull long[] input) {
        return hashLongs(input, 0, input.length);
    }

    /**
     * Returns the hash code for bytes, as they lay in memory, of the specified subsequence
     * of the given {@code long} array.
     *
     * @param input the array to read data from
     * @param off index of the first {@code long} in the subsequence to hash
     * @param len length of the subsequence to hash, in longs (i. e. the length of the bytes
     *            sequence to hash is {@code len * 8L})
     * @return hash code for the specified subsequence
     * @throws IndexOutOfBoundsException if {@code off < 0} or {@code off + len > input.length}
     * or {@code len < 0}
     */
    public long hashLongs(@NotNull long[] input, int off, int len) {
        checkArrayOffs(input.length, off, len);
        return unsafeHash(input, LONG_BASE + (off * 8L), len * 8L);
    }
}
