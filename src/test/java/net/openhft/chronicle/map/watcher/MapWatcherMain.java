/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.map.watcher;

import io.hawt.embedded.Main;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.watcher.ClassifyingWatcherListener;
import net.openhft.chronicle.core.watcher.FileSystemWatcher;

import java.io.File;

public class MapWatcherMain {
    public static void main(String[] args) throws Exception {
        FileSystemWatcher fsw = new FileSystemWatcher();
        String absolutePath = new File(".").getAbsolutePath();
        System.out.println("Watching " + absolutePath);
        fsw.addPath(absolutePath);
        ClassifyingWatcherListener listener = new ClassifyingWatcherListener();
        listener.addClassifier(new MapFileClassifier());
        fsw.addListener(listener);
        fsw.start();

        System.setProperty("hawtio.authenticationEnabled", "false");
        Main main = new Main();
        main.setWar(Jvm.userHome() +"/OpenHFT/hawtio-default-2.7.1.war");
        main.run();
    }
}
