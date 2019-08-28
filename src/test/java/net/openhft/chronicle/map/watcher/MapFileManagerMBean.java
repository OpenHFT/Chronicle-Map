/*
 * Copyright (c) 2016-2019 Chronicle Software Ltd
 */

package net.openhft.chronicle.map.watcher;

import java.lang.reflect.Type;
import java.util.Set;

@SuppressWarnings("unused")
public interface MapFileManagerMBean {

    public String getHeader();

    public String getKeyClass();

    public String getValueClass();

//    public long getSize();

    public String getName();

    public long getDataStoreSize();

    public int getSegments();

    public long getLastUpdate();
}
