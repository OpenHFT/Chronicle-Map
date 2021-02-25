package net.openhft.chronicle.map;

import net.openhft.chronicle.values.Group;
import net.openhft.chronicle.values.Range;
import net.openhft.chronicle.values.Values;

/**
 * Extends segment headers offset from int to long - it may be greater than 4GB for large data stores.
 */
public interface ReplicatedGlobalMutableStateV2 extends ReplicatedGlobalMutableState {
    static void main(String[] args) {
        System.setProperty("chronicle.values.dumpCode", "true");
        Values.nativeClassFor(ReplicatedGlobalMutableStateV2.class);
    }

    @Group(9)
    long getSegmentHeadersOffset();

    void setSegmentHeadersOffset(@Range(min = 0/*, max = 9223372036854775807L*/) long segmentHeadersOffset);
}
