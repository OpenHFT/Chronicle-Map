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
public class CharSequenceConverter implements Converter {

    /**
     * XStream converter for {@link CharSequence} types.
     * <p>
     * Values are written as simple text nodes and read back either as a
     * {@link StringBuilder} when the required type requests it or as a
     * {@link String} for all other {@code CharSequence} subtypes. This mirrors
     * Chronicle's preference for {@code StringBuilder} in performance-sensitive
     * paths while remaining compatible with plain strings.
     */
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
