package net.openhft.chronicle.hash.impl.stage.entry;

import net.openhft.sg.Stage;
import net.openhft.sg.Staged;

@Staged
public abstract class HashLookupPos {

    public long hashLookupPos = -1;

    public abstract boolean hashLookupPosInit();

    public void initHashLookupPos(long hashLookupPos) {
        this.hashLookupPos = hashLookupPos;
    }

    @Stage("HashLookupPos")
    public void setHashLookupPos(long hashLookupPos) {
        this.hashLookupPos = hashLookupPos;
    }
    
    public abstract void closeHashLookupPos();
}
