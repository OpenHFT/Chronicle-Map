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

package net.openhft.chronicle.map.fromdocs;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.StatefulCopyable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

public final class CharSequenceCustomEncodingBytesReader
        implements BytesReader<CharSequence>,
        StatefulCopyable<CharSequenceCustomEncodingBytesReader> {

    // config fields, non-final because read in readMarshallable()
    private Charset charset;
    private int inputBufferSize;

    // cache fields
    private transient CharsetDecoder charsetDecoder;
    private transient ByteBuffer inputBuffer;
    private transient CharBuffer outputBuffer;

    public CharSequenceCustomEncodingBytesReader(Charset charset, int inputBufferSize) {
        this.charset = charset;
        this.inputBufferSize = inputBufferSize;
        initTransients();
    }

    private void initTransients() {
        charsetDecoder = charset.newDecoder();
        inputBuffer = ByteBuffer.allocate(inputBufferSize);
        int outputBufferSize = (int) (inputBufferSize * charsetDecoder.averageCharsPerByte());
        outputBuffer = CharBuffer.allocate(outputBufferSize);
    }

    @NotNull
    @Override
    public CharSequence read(Bytes in, @Nullable CharSequence using) {
        long csLengthAsLong = in.readStopBit();
        if (csLengthAsLong > Integer.MAX_VALUE) {
            throw new IORuntimeException("cs len shouldn't be more than " + Integer.MAX_VALUE +
                    ", " + csLengthAsLong + " read");
        }
        int csLength = (int) csLengthAsLong;
        StringBuilder sb;
        if (using instanceof StringBuilder) {
            sb = (StringBuilder) using;
            sb.setLength(0);
            sb.ensureCapacity(csLength);
        } else {
            sb = new StringBuilder(csLength);
        }

        int remainingBytes = in.readInt();
        charsetDecoder.reset();
        inputBuffer.clear();
        outputBuffer.clear();
        boolean endOfInput = false;
        // this loop inspired by the CharsetDecoder.decode(ByteBuffer) implementation
        while (true) {
            if (!endOfInput) {
                int inputChunkSize = Math.min(inputBuffer.remaining(), remainingBytes);
                inputBuffer.limit(inputBuffer.position() + inputChunkSize);
                in.read(inputBuffer);
                inputBuffer.flip();
                remainingBytes -= inputChunkSize;
                endOfInput = remainingBytes == 0;
            }

            CoderResult cr = inputBuffer.hasRemaining() ?
                    charsetDecoder.decode(inputBuffer, outputBuffer, endOfInput) :
                    CoderResult.UNDERFLOW;

            if (cr.isUnderflow() && endOfInput)
                cr = charsetDecoder.flush(outputBuffer);

            if (cr.isUnderflow()) {
                if (endOfInput) {
                    break;
                } else {
                    inputBuffer.compact();
                    continue;
                }
            }

            if (cr.isOverflow()) {
                outputBuffer.flip();
                sb.append(outputBuffer);
                outputBuffer.clear();
                continue;
            }

            try {
                cr.throwException();
            } catch (CharacterCodingException e) {
                throw new IORuntimeException(e);
            }
        }
        outputBuffer.flip();
        sb.append(outputBuffer);

        return sb;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) throws IORuntimeException {
        charset = (Charset) wireIn.read(() -> "charset").object();
        inputBufferSize = wireIn.read(() -> "inputBufferSize").int32();
        initTransients();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        wireOut.write(() -> "charset").object(charset);
        wireOut.write(() -> "inputBufferSize").int32(inputBufferSize);
    }

    @Override
    public CharSequenceCustomEncodingBytesReader copy() {
        return new CharSequenceCustomEncodingBytesReader(charset, inputBufferSize);
    }
}
