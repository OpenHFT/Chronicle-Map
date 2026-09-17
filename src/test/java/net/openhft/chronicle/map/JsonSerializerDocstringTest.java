/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.map;

import com.thoughtworks.xstream.XStream;
import org.codehaus.jettison.mapped.MappedXMLStreamReader;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the dependency versions advertised in
 * {@link JsonSerializer#LOG_ERROR_SUGGEST_X_STREAM} to the actual versions
 * resolved on the test classpath.
 * <p>
 * The error message is a documented contract: anyone hitting it copies the
 * snippet straight into their {@code pom.xml}. If the BOM bumps xstream or
 * jettison and this docstring is not kept in sync, users would be advised to
 * pin a stale (potentially vulnerable) version. These tests fail loudly on
 * such drift.
 */
class JsonSerializerDocstringTest {

    private static final Pattern XSTREAM_VERSION =
            Pattern.compile("xstream</artifactId>\\s*<version>([^<]+)</version>");
    private static final Pattern JETTISON_VERSION =
            Pattern.compile("jettison</artifactId>\\s*<version>([^<]+)</version>");

    private static String docstring() throws Exception {
        Field f = JsonSerializer.class.getDeclaredField("LOG_ERROR_SUGGEST_X_STREAM");
        f.setAccessible(true);
        assertTrue(Modifier.isStatic(f.getModifiers()),
                "docstring constant should be static");
        Object v = f.get(null);
        assertNotNull(v);
        return v.toString();
    }

    @Test
    void xstreamVersionInDocstringMatchesClasspath() throws Exception {
        String advertised = extract(XSTREAM_VERSION, docstring());
        // XStream exposes its version as a public static field.
        String actual = XStream.class.getPackage().getImplementationVersion();
        if (actual == null) {
            // Fall back to xstream's META-INF / pom.properties when running
            // from an exploded build where Implementation-Version is absent.
            actual = readPomPropertiesVersion(
                    XStream.class,
                    "META-INF/maven/com.thoughtworks.xstream/xstream/pom.properties");
        }
        assertNotNull(actual, "could not determine xstream classpath version");
        assertEquals(actual, advertised,
                "docstring xstream version drifted from classpath");
    }

    @Test
    void jettisonVersionInDocstringMatchesClasspath() throws Exception {
        String advertised = extract(JETTISON_VERSION, docstring());
        String actual = MappedXMLStreamReader.class.getPackage().getImplementationVersion();
        if (actual == null) {
            actual = readPomPropertiesVersion(
                    MappedXMLStreamReader.class,
                    "META-INF/maven/org.codehaus.jettison/jettison/pom.properties");
        }
        assertNotNull(actual, "could not determine jettison classpath version");
        assertEquals(actual, advertised,
                "docstring jettison version drifted from classpath. "
                        + "JsonSerializer.LOG_ERROR_SUGGEST_X_STREAM tells users to "
                        + "pin " + advertised + " but the classpath resolves to " + actual);
    }

    @Test
    void xstreamGroupIdIsCorrectInDocstring() throws Exception {
        String s = docstring();
        assertTrue(s.contains("<groupId>com.thoughtworks.xstream</groupId>"),
                "docstring must reference the correct xstream groupId; "
                        + "the original snippet had a bare 'xstream' which is wrong: " + s);
    }

    private static String extract(Pattern p, String haystack) {
        Matcher m = p.matcher(haystack);
        assertTrue(m.find(), "pattern " + p + " did not match docstring: " + haystack);
        return m.group(1).trim();
    }

    private static String readPomPropertiesVersion(Class<?> c, String resource)
            throws IOException {
        try (InputStream in = c.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) return null;
            Properties p = new Properties();
            p.load(in);
            return p.getProperty("version");
        }
    }
}
