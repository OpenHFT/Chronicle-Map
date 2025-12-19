/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map.fromdocs;

import net.openhft.chronicle.map.ChronicleMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class CustomCharSequenceEncodingTest {

    @Test
    public void customCharSequenceEncodingTest() {
        Charset charset = StandardCharsets.UTF_8;
        int charBufferSize = 4;
        int bytesBufferSize = 8;
        CharSequenceCustomEncodingBytesWriter writer =
                new CharSequenceCustomEncodingBytesWriter(charset, charBufferSize);
        CharSequenceCustomEncodingBytesReader reader =
                new CharSequenceCustomEncodingBytesReader(charset, bytesBufferSize);
        try (ChronicleMap<String, CharSequence> map = ChronicleMap
                .of(String.class, CharSequence.class)
                .valueMarshallers(reader, writer)
                .averageKey("Russian")
                .averageValue("Всем нравится субботний вечерок")
                .entries(10)
                .create()) {
            map.put("Russian", "Всем нравится субботний вечерок");
            map.put("", "Quick brown fox jumps over the lazy dog");
            Assertions.assertEquals("Всем нравится субботний вечерок", map.get("Russian").toString(),
                    "UTF-8 Russian roundtrip");
            Assertions.assertEquals("Quick brown fox jumps over the lazy dog", map.get("").toString(),
                    "UTF-8 English roundtrip");
        }
    }

    @Test
    public void gbkCharSequenceEncodingTest() {
        Charset charset = Charset.forName("GBK");
        int charBufferSize = 100;
        int bytesBufferSize = 200;
        CharSequenceCustomEncodingBytesWriter writer =
                new CharSequenceCustomEncodingBytesWriter(charset, charBufferSize);
        CharSequenceCustomEncodingBytesReader reader =
                new CharSequenceCustomEncodingBytesReader(charset, bytesBufferSize);
        try (ChronicleMap<String, CharSequence> englishToChinese = ChronicleMap
                .of(String.class, CharSequence.class)
                .valueMarshallers(reader, writer)
                .averageKey("hello")
                .averageValue("你好")
                .entries(10)
                .create()) {
            englishToChinese.put("hello", "你好");
            englishToChinese.put("bye", "再见");

            Assertions.assertEquals("你好", englishToChinese.get("hello").toString(),
                    "GBK hello roundtrip");
            Assertions.assertEquals("再见", englishToChinese.get("bye").toString(),
                    "GBK bye roundtrip");
        }
    }
}
