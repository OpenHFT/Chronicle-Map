/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package net.openhft.chronicle.hash.impl.util;

import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * gets the version of the current build
 */
public final class BuildVersion {

    private static String version = null;

    private BuildVersion() {
    }

    public static void main(String[] args) {
        System.out.println(version());
    }

    /**
     * @return version of ChronicleMap being used, or NULL if its not known
     */
    public static synchronized String version() {

        if (version != null) {
            return version;
        }

        try {
            // the best way to get the version is to read the map.version file
            InputStream resource = BuildVersion.class.getClassLoader().getResourceAsStream("map" +
                    ".version");
            BufferedReader in = new BufferedReader(new InputStreamReader(resource, StandardCharsets.UTF_8));

            version = in.readLine().trim();
            if (!"${project.version}".equals(version()))
                return version;

            return version;
        } catch (Exception e) {
            // do nothing
        }

        // another way to get the version is to read it from the manifest
        final String versionFromManifest = getVersionFromManifest();

        if (versionFromManifest != null) {
            version = versionFromManifest;
            return version;
        }

        return null;
    }

    /**
     * This should be used by everyone that has install chronicle map as a JAR
     *
     * @return gets the version out of the manifest, or null if it can not be read
     */
    private static String getVersionFromManifest() {
        return ChronicleMapBuilder.class.getPackage().getImplementationVersion();
    }
}
