package net.openhft.chronicle.map;

import net.openhft.chronicle.hash.VanillaGlobalMutableState;
import net.openhft.chronicle.values.*;

/**
 * Extends segment headers offset from int to long - it may be greater than 4GB for large data stores.
 */
public interface ReplicatedGlobalMutableStateV2 extends VanillaGlobalMutableState {

    static void main(String[] args) {
        System.setProperty("chronicle.values.dumpCode", "true");
        Values.nativeClassFor(ReplicatedGlobalMutableStateV2.class);
    }

    @Group(6)
    int getCurrentCleanupSegmentIndex();

    void setCurrentCleanupSegmentIndex(
            @Range(min = 0, max = Integer.MAX_VALUE) int currentCleanupSegmentIndex);

    @Group(7)
    @Align(offset = 1)
    int getModificationIteratorsCount();

    int addModificationIteratorsCount(@Range(min = 0, max = 128) int addition);

    @Group(8)
    // Align to prohibit array filling the highest bit of the previous 2-byte block, that
    // complicates generated code for all fields, defined in this interface.
    @Align(offset = 2)
    @Array(length = 128)
    boolean getModificationIteratorInitAt(int index);

    void setModificationIteratorInitAt(int index, boolean init);

    @Group(9)
    long getSegmentHeadersOffset();

    void setSegmentHeadersOffset(@Range(min = 0/*, max = 9223372036854775807L*/) long segmentHeadersOffset);
}
