/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.map.watcher;

import net.openhft.chronicle.core.watcher.FileClassifier;
import net.openhft.chronicle.core.watcher.FileManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class MapFileClassifier implements FileClassifier {
    @Override
    public FileManager classify(String base, String relative) {
        Path path = Paths.get(base, relative);
        if (Files.isRegularFile(path) && relative.endsWith(".cmap")) {
            return new MapFileManager(base, relative);
        }
        return null;
    }
}
