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
package net.openhft.chronicle.map;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;
import net.openhft.xstreem.convertors.ByteBufferConverter;
import net.openhft.xstreem.convertors.ChronicleMapConverter;
import net.openhft.xstreem.convertors.DataValueConverter;

import java.io.*;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author Rob Austin.
 */
class JsonSerializer {

    static synchronized <K, V> void getAll(File toFile, Map<K, V> map) throws IOException {
        final XStream xstream = new XStream(new JettisonMappedXmlDriver());
        xstream.setMode(XStream.NO_REFERENCES);
        xstream.alias("cmap", VanillaChronicleMap.EntrySet.class);

        xstream.registerConverter(new ChronicleMapConverter<K, V>(map));
        xstream.registerConverter(new ByteBufferConverter());
        xstream.registerConverter(new DataValueConverter());
        OutputStream outputStream = new FileOutputStream(toFile);
        if (toFile.getName().toLowerCase().endsWith(".gz"))
            outputStream = new GZIPOutputStream(outputStream);
        try (OutputStream out = outputStream) {
            xstream.toXML(map.entrySet(), out);
        }
    }


    static synchronized <K, V> void putAll(File fromFile, Map<K, V> map) throws IOException {
        final XStream xstream = new XStream(new JettisonMappedXmlDriver());
        xstream.setMode(XStream.NO_REFERENCES);
        xstream.alias("cmap", VanillaChronicleMap.EntrySet.class);
        xstream.registerConverter(new ChronicleMapConverter<K, V>(map));
        xstream.registerConverter(new ByteBufferConverter());
        xstream.registerConverter(new DataValueConverter());

        InputStream inputStream = new FileInputStream(fromFile);
        if (fromFile.getName().toLowerCase().endsWith(".gz"))
            inputStream = new GZIPInputStream(inputStream);
        try (InputStream out = inputStream) {
            xstream.fromXML(out);
        }
    }
}
