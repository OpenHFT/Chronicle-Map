/*
 * Copyright 2014 Higher Frequency Trading
 *
 * http://www.higherfrequencytrading.com
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

package net.openhft.chronicle.map.fromdocs;

import net.openhft.lang.threadlocal.StatefulCopyable;
import net.openhft.chronicle.set.ChronicleSet;
import net.openhft.chronicle.set.ChronicleSetBuilder;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.CharBuffers;
import net.openhft.lang.io.serialization.BytesMarshaller;
import net.openhft.lang.model.constraints.NotNull;
import net.openhft.lang.pool.CharSequenceInterner;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.*;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

public class StringEncodingExamples {

    static class CustomEncodingMarshaller implements BytesMarshaller<CharSequence>,
            StatefulCopyable<CustomEncodingMarshaller> {
        private static final long serialVersionUID = 0L;

        private String charsetName;
        private transient Charset charset;
        private transient CharsetEncoder encoder;
        private transient CharsetDecoder decoder;
        private transient ByteBuffer bytesAsBuffer;
        private transient CharBuffer charSequenceAsBuffer;
        private transient CharBuffer returnedCharBuffer;

        CustomEncodingMarshaller(Charset charset) {
            this.charsetName = charset.name().intern();
            this.charset = charset;
            initCoders();
        }

        CustomEncodingMarshaller(CustomEncodingMarshaller m) {
            charsetName = m.charsetName;
            charset = m.charset;
            encoder = m.encoder;
            decoder = m.decoder;
        }

        private void initCoders() {
            encoder = charset.newEncoder();
            decoder = charset.newDecoder();
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            charsetName = charsetName.intern();
            charset = Charset.forName(charsetName);
            initCoders();
        }

        @Override
        public void write(Bytes bytes, CharSequence cs) {
            bytes.writeStopBit(cs.length());
            bytesAsBuffer = bytes.sliceAsByteBuffer(bytesAsBuffer);
            charSequenceAsBuffer = CharBuffers.wrap(cs, charSequenceAsBuffer);
            encoder.encode(charSequenceAsBuffer, bytesAsBuffer, true);
            bytes.position(bytes.position() + bytesAsBuffer.position());
        }

        @Override
        public CharSequence read(Bytes bytes) {
            int csLen = (int) bytes.readStopBit();
            if (returnedCharBuffer == null || returnedCharBuffer.capacity() < csLen) {
                returnedCharBuffer = CharBuffer.allocate(csLen);
            } else {
                returnedCharBuffer.clear().limit(csLen);
            }
            bytesAsBuffer = bytes.sliceAsByteBuffer(bytesAsBuffer);
            decoder.decode(bytesAsBuffer, returnedCharBuffer, true);
            bytes.position(bytes.position() + bytesAsBuffer.position());
            returnedCharBuffer.flip();
            return returnedCharBuffer;
        }

        @Override
        public CharSequence read(Bytes bytes, @Nullable CharSequence cs) {
            return read(bytes);
        }

        @Override
        public Object stateIdentity() {
            return charsetName;
        }

        @Override
        public CustomEncodingMarshaller copy() {
            return new CustomEncodingMarshaller(this);
        }
    }

    static class Utf16Marshaller<CS extends CharSequence> implements BytesMarshaller<CS>,
            StatefulCopyable<Utf16Marshaller<CS>> {
        private static final long serialVersionUID = 0L;

        private final Serializable identity;
        private final CharSequenceInterner<CS> interner;
        private transient char[] readBuffer;

        <I extends CharSequenceInterner<CS> & Serializable>
        Utf16Marshaller(Serializable identity, I interner) {
            this.identity = identity;
            this.interner = interner;
        }

        @Override
        public void write(Bytes bytes, CS cs) {
            bytes.writeCompactInt(cs.length());
            bytes.writeChars(cs);
        }

        @Override
        public CS read(Bytes bytes) {
            return read(bytes, null);
        }

        @Override
        public CS read(Bytes bytes, @Nullable CS cs) {
            int csLen = bytes.readCompactInt();
            if (readBuffer == null || readBuffer.length < csLen)
                readBuffer = new char[csLen];
            bytes.readFully(readBuffer, 0, csLen);
            return interner.intern(CharBuffer.wrap(readBuffer, 0, csLen));
        }

        @Override
        public Object stateIdentity() {
            return identity;
        }

        @Override
        public Utf16Marshaller<CS> copy() {
            return new Utf16Marshaller(identity, interner);
        }
    }

    enum NoInterning implements CharSequenceInterner<CharSequence> {
        INSTANCE;

        @Override
        public CharSequence intern(@NotNull CharSequence cs) {
            return cs;
        }
    }

    @Test
    public void testCustomEncodingMarshaller() throws IOException {
        CustomEncodingMarshaller gbkStringMarshaller =
                new CustomEncodingMarshaller(Charset.forName("GBK"));
        testCustomKeyMarshaller(gbkStringMarshaller);
    }


    @Test
    public void testNoEncodingMarshaller() throws IOException {
        Utf16Marshaller<CharSequence> noEncodingMarshaller =
                new Utf16Marshaller<CharSequence>(NoInterning.INSTANCE, NoInterning.INSTANCE);
        testCustomKeyMarshaller(noEncodingMarshaller);
    }

    private void testCustomKeyMarshaller(BytesMarshaller<CharSequence> marshaller)
            throws IOException {
        ChronicleSet<CharSequence> chineseWordSet = ChronicleSetBuilder.of(CharSequence.class)
                .keyMarshaller(marshaller)
                .actualSegments(1)
                .actualEntriesPerSegment(1000)
                .create();

        chineseWordSet.add("新闻");
        chineseWordSet.add("地图");
        chineseWordSet.add("贴吧");
        chineseWordSet.add("登录");
        chineseWordSet.add("设置");
        Assert.assertEquals(5, chineseWordSet.size());
        System.out.println(chineseWordSet);
    }
}
