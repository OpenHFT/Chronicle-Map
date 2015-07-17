package net.openhft.chronicle.hash.impl.stage.query;

import net.openhft.chronicle.hash.impl.stage.entry.HashLookupPos;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public abstract class QueryHashLookupPos extends HashLookupPos {
    
    @StageRef SegmentStages s;
    @StageRef HashLookupSearch hashLookupSearch;
    
    public void initHashLookupPos() {
        s.innerReadLock.lock();
        this.hashLookupPos = hashLookupSearch.searchStartPos;
    }
}
