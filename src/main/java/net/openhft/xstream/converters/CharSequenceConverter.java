/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.xstream.converters;

import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;

/**
 * XStream converter that handles {@link CharSequence} implementations.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class CharSequenceConverter implements Converter {

    /**
     * Creates a converter for {@link CharSequence} values.
     */
    public CharSequenceConverter() {
    }

    @Override
    public void marshal(
            Object source, HierarchicalStreamWriter writer, MarshallingContext context) {
        writer.setValue(source.toString());
    }

    @Override
    public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
        if (context.getRequiredType() == StringBuilder.class) {
            return new StringBuilder(reader.getValue());
        } else {
            return reader.getValue();
        }
    }

    @Override
    public boolean canConvert(Class type) {
        return CharSequence.class.isAssignableFrom(type);
    }
}
