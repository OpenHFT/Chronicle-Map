/*
 * Copyright (c) 2016-2020 chronicle.software
 */

package net.openhft.chronicle.map.watcher;

@SuppressWarnings("unused")
@Deprecated(/* for removal in x.24 */)
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
