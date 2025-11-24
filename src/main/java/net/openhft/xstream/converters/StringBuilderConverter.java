/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.xstream.converters;

import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;

@SuppressWarnings({"rawtypes", "unchecked"})
public class StringBuilderConverter implements Converter {

    /**
     * XStream converter specialising in {@link StringBuilder}.
     * <p>
     * Serialises the builder's contents as a plain text node and reconstructs
     * a new {@code StringBuilder} instance on deserialisation. This avoids
     * accidental conversion to immutable {@link String} while remaining
     * compatible with XStream's converter SPI.
     */
    @Override
    public void marshal(
            Object source, HierarchicalStreamWriter writer, MarshallingContext context) {
        writer.setValue(source.toString());
    }

    @Override
    public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
        return new StringBuilder(reader.getValue());
    }

    @Override
    public boolean canConvert(Class type) {
        return type == StringBuilder.class;
    }
}
