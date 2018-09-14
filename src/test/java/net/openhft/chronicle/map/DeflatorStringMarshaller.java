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

package net.openhft.chronicle.map;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.impl.EnumMarshallable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.lang.reflect.Constructor;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Created by Peter Lawrey on 24/10/14.
 */
public final class DeflatorStringMarshaller implements BytesReader<CharSequence>,
        BytesWriter<CharSequence>, EnumMarshallable<DeflatorStringMarshaller> {
    public static final DeflatorStringMarshaller INSTANCE = new DeflatorStringMarshaller();
    private static final StringFactory STRING_FACTORY = getStringFactory();
    private static final int NULL_LENGTH = -1;

    private DeflatorStringMarshaller() {
    }

    private static StringFactory getStringFactory() {
        try {
            return new StringFactory17();
        } catch (Exception e) {
            // do nothing
        }

        try {
            return new StringFactory16();
        } catch (Exception e) {
            // no more alternatives
            throw new AssertionError(e);
        }
    }

    @Override
    public void write(Bytes out, @NotNull CharSequence s) {
        if (s == null) {
            out.writeStopBit(NULL_LENGTH);
            return;
        } else if (s.length() == 0) {
            out.writeStopBit(0);
            return;
        }
        // write the total length.
        int length = s.length();
        out.writeStopBit(length);

        long position = out.writePosition();
        out.writeInt(0);
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
                new DeflaterOutputStream(out.outputStream())));
        try {
            for (int i = 0; i < s.length(); i++) {
                dos.write(s.charAt(i));
            }
            dos.close();
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
        out.writeInt(position, (int) (out.writePosition() - position - 4));
    }

    @NotNull
    @Override
    public CharSequence read(Bytes bytes, @Nullable CharSequence ignored) {
        long size = bytes.readStopBit();
        if (size == NULL_LENGTH)
            return null;
        if (size < 0 || size > Integer.MAX_VALUE)
            throw new IllegalStateException("Invalid length: " + size);
        if (size == 0)
            return "";

        char[] chars = new char[(int) size];
        int length = bytes.readInt();
        long limit = bytes.readLimit();
        try {
            bytes.readLimit(bytes.readPosition() + length);
            DataInputStream dis = new DataInputStream(new BufferedInputStream(
                    new InflaterInputStream(bytes.inputStream())));
            for (int i = 0; i < size; i++)
                chars[i] = (char) (dis.readByte() & 0xff);

        } catch (IOException e) {
            throw new IORuntimeException(e);
        } finally {
            bytes.readPosition(bytes.readLimit());
            bytes.readLimit(limit);
        }
        try {
            return STRING_FACTORY.fromChars(chars);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public DeflatorStringMarshaller readResolve() {
        return INSTANCE;
    }

    private static abstract class StringFactory {
        abstract String fromChars(char[] chars) throws Exception;
    }

    private static final class StringFactory16 extends StringFactory {
        private final Constructor<String> constructor;

        private StringFactory16() throws Exception {
            constructor = String.class.getDeclaredConstructor(int.class,
                    int.class, char[].class);
            constructor.setAccessible(true);
        }

        @Override
        String fromChars(char[] chars) throws Exception {
            return constructor.newInstance(0, chars.length, chars);
        }
    }

    private static final class StringFactory17 extends StringFactory {
        private final Constructor<String> constructor;

        private StringFactory17() throws Exception {
            constructor = String.class.getDeclaredConstructor(char[].class, boolean.class);
            constructor.setAccessible(true);
        }

        @Override
        String fromChars(char[] chars) throws Exception {
            return constructor.newInstance(chars, true);
        }
    }
}
