/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.xstream.converters;

import com.thoughtworks.xstream.converters.ConversionException;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;

/**
 * Created by Rob Austin
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ByteBufferConverter implements Converter {

    private final Charset charset = Charset.forName("ISO-8859-1");
    private final CharsetDecoder decoder = charset.newDecoder();

    @Override
    public void marshal(Object o, HierarchicalStreamWriter writer, MarshallingContext marshallingContext) {
        ByteBuffer buffer = (ByteBuffer) o;

        writer.startNode("position");
        marshallingContext.convertAnother(buffer.position());
        writer.endNode();

        writer.startNode("capacity");
        marshallingContext.convertAnother(buffer.capacity());
        writer.endNode();

        writer.startNode("limit");
        marshallingContext.convertAnother(buffer.limit());
        writer.endNode();

        writer.startNode("isDirect");
        marshallingContext.convertAnother(buffer.isDirect());
        writer.endNode();

        buffer.limit();
        buffer.capacity();

        int position = buffer.position();
        final int limit = buffer.limit();

        buffer.clear();

        writer.startNode("data");

        try {
            CharBuffer charBuffer = decoder.decode(buffer);
            writer.setValue(charBuffer.toString());
        } catch (CharacterCodingException e) {
            throw new ConversionException("", e);
        }

        writer.endNode();

        buffer.limit(limit);
        buffer.position(position);
    }

    @Override
    public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext unmarshallingContext) {

        reader.moveDown();
        int position = (Integer) unmarshallingContext.convertAnother(null, int.class);
        reader.moveUp();

        reader.moveDown();
        int capacity = (Integer) unmarshallingContext.convertAnother(null, int.class);
        reader.moveUp();

        reader.moveDown();
        final int limit = (Integer) unmarshallingContext.convertAnother(null, int.class);
        reader.moveUp();

        reader.moveDown();
        boolean isDirect = (Boolean) unmarshallingContext.convertAnother(null, boolean.class);
        reader.moveUp();

        ByteBuffer buffer = isDirect ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
        buffer.clear();

        reader.moveDown();

        String o = (String) unmarshallingContext.convertAnother(null, String.class);

        CharBuffer uCharBuffer = CharBuffer.wrap(o);

        CharsetEncoder encoder = charset.newEncoder();
        CoderResult encode = encoder.encode(uCharBuffer, buffer, true);
        if (encode.isError())
            throw new ConversionException("");

        buffer.limit(limit);
        buffer.position(position);
        reader.moveUp();

        buffer.limit(limit);
        buffer.position(position);
        return buffer;
    }

    @Override
    public boolean canConvert(Class aClass) {
        return ByteBuffer.class.isAssignableFrom(aClass);
    }
}
