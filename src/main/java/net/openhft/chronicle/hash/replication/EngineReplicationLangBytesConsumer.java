package net.openhft.chronicle.hash.replication;

import net.openhft.chronicle.map.EngineReplicationLangBytes;
import org.jetbrains.annotations.NotNull;

/**
 * Created by Rob Austin
 */
public interface EngineReplicationLangBytesConsumer {

    void set(@NotNull EngineReplicationLangBytes engineReplicationLangBytes);
}
