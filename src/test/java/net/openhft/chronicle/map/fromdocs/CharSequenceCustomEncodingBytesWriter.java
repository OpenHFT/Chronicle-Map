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

package net.openhft.chronicle.map.fromdocs;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.StatefulCopyable;
import net.openhft.chronicle.wire.WireIn;
import net.openhft.chronicle.wire.WireOut;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;

public final class CharSequenceCustomEncodingBytesWriter
        implements BytesWriter<CharSequence>,
        StatefulCopyable<CharSequenceCustomEncodingBytesWriter> {

    // config fields, non-final because read in readMarshallable()
    private Charset charset;
    private int inputBufferSize;

    // cache fields
    private transient CharsetEncoder charsetEncoder;
    private transient CharBuffer inputBuffer;
    private transient ByteBuffer outputBuffer;

    public CharSequenceCustomEncodingBytesWriter(Charset charset, int inputBufferSize) {
        this.charset = charset;
        this.inputBufferSize = inputBufferSize;
        initTransients();
    }

    /**
     * Need this method because {@link CharBuffer#append(CharSequence, int, int)} produces garbage
     */
    private static void append(CharBuffer charBuffer, CharSequence cs, int start, int end) {
        for (int i = start; i < end; i++) {
            charBuffer.put(cs.charAt(i));
        }
    }

    private void initTransients() {
        charsetEncoder = charset.newEncoder();
        inputBuffer = CharBuffer.allocate(inputBufferSize);
        int outputBufferSize = (int) (inputBufferSize * charsetEncoder.averageBytesPerChar());
        outputBuffer = ByteBuffer.allocate(outputBufferSize);
    }

    @Override
    public void write(Bytes out, @NotNull CharSequence cs) {
        // Write the actual cs length for accurate StringBuilder.ensureCapacity() while reading
        out.writeStopBit(cs.length());
        long encodedSizePos = out.writePosition();
        out.writeSkip(4);
        charsetEncoder.reset();
        inputBuffer.clear();
        outputBuffer.clear();
        int csPos = 0;
        boolean endOfInput = false;
        // this loop inspired by the CharsetEncoder.encode(CharBuffer) implementation
        while (true) {
            if (!endOfInput) {
                int nextCsPos = Math.min(csPos + inputBuffer.remaining(), cs.length());
                append(inputBuffer, cs, csPos, nextCsPos);
                inputBuffer.flip();
                endOfInput = nextCsPos == cs.length();
                csPos = nextCsPos;
            }

            CoderResult cr = inputBuffer.hasRemaining() ?
                    charsetEncoder.encode(inputBuffer, outputBuffer, endOfInput) :
                    CoderResult.UNDERFLOW;

            if (cr.isUnderflow() && endOfInput)
                cr = charsetEncoder.flush(outputBuffer);

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
                writeOutputBuffer(out);
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
        writeOutputBuffer(out);

        out.writeInt(encodedSizePos, (int) (out.writePosition() - encodedSizePos - 4));
    }

    private void writeOutputBuffer(Bytes out) {
        int remaining = outputBuffer.remaining();
        out.write(out.writePosition(), outputBuffer, 0, remaining);
        out.writeSkip(remaining);
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
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
    public CharSequenceCustomEncodingBytesWriter copy() {
        return new CharSequenceCustomEncodingBytesWriter(charset, inputBufferSize);
    }
}
