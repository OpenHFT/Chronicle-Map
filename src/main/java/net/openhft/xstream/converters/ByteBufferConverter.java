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
        int limit = buffer.limit();

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
        int limit = (Integer) unmarshallingContext.convertAnother(null, int.class);
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
