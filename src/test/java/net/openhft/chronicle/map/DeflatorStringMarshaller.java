package net.openhft.chronicle.map;

import net.openhft.chronicle.bytes.IORuntimeException;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.CompactBytesMarshaller;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.lang.reflect.Constructor;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Created by peter on 24/10/14.
 */
public enum DeflatorStringMarshaller implements CompactBytesMarshaller<CharSequence> {
    INSTANCE;

    private static final StringFactory STRING_FACTORY = getStringFactory();

    private static final int NULL_LENGTH = -1;

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
    public byte code() {
        return '_';
    }

    @Override
    public void write(Bytes bytes, CharSequence s) {
        if (s == null) {
            bytes.writeStopBit(NULL_LENGTH);
            return;

        } else if (s.length() == 0) {
            bytes.writeStopBit(0);
            return;
        }
        // write the total length.
        int length = s.length();
        bytes.writeStopBit(length);

        long position = bytes.position();
        bytes.writeInt(0);
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(
                new DeflaterOutputStream(bytes.outputStream())));
        try {
            for (int i = 0; i < s.length(); i++) {
                dos.write(s.charAt(i));
            }
            dos.close();
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
        bytes.writeInt(position, (int) (bytes.position() - position - 4));
    }

    @Override
    public String read(Bytes bytes) {
        return read(bytes, null);
    }

    @Override
    public String read(Bytes bytes, @Nullable CharSequence ignored) {
        long size = bytes.readStopBit();
        if (size == NULL_LENGTH)
            return null;
        if (size < 0 || size > Integer.MAX_VALUE)
            throw new IllegalStateException("Invalid length: " + size);
        if (size == 0)
            return "";

        char[] chars = new char[(int) size];
        int length = bytes.readInt();
        long limit = bytes.limit();
        try {
            bytes.limit(bytes.position() + length);
            DataInputStream dis = new DataInputStream(new BufferedInputStream(
                    new InflaterInputStream(bytes.inputStream())));
            for (int i = 0; i < size; i++)
                chars[i] = (char) (dis.readByte() & 0xff);
        } catch (IOException e) {
            throw new IORuntimeException(e);
        } finally {
            bytes.position(bytes.limit());
            bytes.limit(limit);
        }
        try {
            return STRING_FACTORY.fromChars(chars);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
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
