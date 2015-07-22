package net.openhft.chronicle.map.impl.stage.query;

import net.openhft.chronicle.hash.Data;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.chronicle.hash.impl.stage.hash.CheckOnEachPublicOperation;
import net.openhft.chronicle.hash.impl.stage.query.HashLookupSearch;
import net.openhft.chronicle.map.MapAbsentEntry;
import net.openhft.chronicle.map.MapContext;
import net.openhft.chronicle.map.impl.VanillaChronicleMapHolder;
import net.openhft.chronicle.map.impl.stage.entry.MapEntryStages;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;
import org.jetbrains.annotations.NotNull;

import static net.openhft.chronicle.hash.impl.stage.query.HashQuery.SearchState.PRESENT;

@Staged
public class MapAbsent<K, V> implements MapAbsentEntry<K, V> {

    @StageRef MapQuery<K, V, ?> q;
    @StageRef MapEntryStages<K, V> e;
    @StageRef public HashLookupSearch hashLookupSearch;
    @StageRef public CheckOnEachPublicOperation checkOnEachPublicOperation;
    @StageRef public SegmentStages s;
    @StageRef VanillaChronicleMapHolder<K, ?, ?, V, ?, ?, ?> mh;
    @StageRef public DeprecatedMapKeyContextOnQuery<K, V> deprecatedMapKeyContext;

    void putEntry(Data<V> value) {
        assert q.searchStateAbsent();
        long entrySize = e.entrySize(q.inputKey.size(), value.size());
        q.allocatedChunks.initEntryAndKey(entrySize);
        e.initValue(value);
        e.freeExtraAllocatedChunks();
        hashLookupSearch.putNewVolatile(e.pos);
    }

    @NotNull
    @Override
    public MapContext<K, V, ?> context() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return q;
    }

    @NotNull
    @Override
    public Data<K> absentKey() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return q.inputKey;
    }

    @Override
    public void doInsert(Data<V> value) {
        q.putPrefix();
        if (!q.searchStatePresent()) {
            if (q.searchStateDeleted()) {
                e.putValueDeletedEntry(value);
            } else {
                putEntry(value);
            }
            s.incrementModCount();
            q.setSearchState(PRESENT);
        } else {
            throw new IllegalStateException(
                    "Entry is present in the map when doInsert() is called");
        }
    }

    @NotNull
    @Override
    public Data<V> defaultValue() {
        checkOnEachPublicOperation.checkOnEachPublicOperation();
        return q.wrapValueAsData(mh.m().defaultValue(deprecatedMapKeyContext));
    }
}
