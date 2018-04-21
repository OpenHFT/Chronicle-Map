# 6. Chronicle Map Data Store Query Operations

Some definitions, used on this page:

##### Entries number field

The *entries number field* of some tier is the 2nd field of the [segment header structure](
3-memory-layout.md#segment-header-structure) (if the tier is the first in the segment it belongs
to), or the 6th field of the [tier counters area](3-memory-layout.md#segment-tier-counters-area)
(if the tier is not the first in the segment it belongs to).

##### Smallest free chunk index field

The *smallest free chunk index field* of some tier is the 3rd field of the [segment header
structure](3-memory-layout.md#segment-header-structure) (if the tier is the first in the segment it
belongs to), or the 3rd field of the [tier counters area](
3-memory-layout.md#segment-tier-counters-area) (if the tier is not the first in the segment it
belongs to).

##### Next tier field

The *next tier field* of some tier is the 4th field of the [segment header structure](
3-memory-layout.md#segment-header-structure) (if the tier is the first in the segment it belongs
to), or the 1st field of the [counters area](3-memory-layout.md#segment-tier-counters-area) (if the
tier belongs to one of [extra tier bulks](3-memory-layout.md#extra-tier-bulks), i. e. not the first
in the segment it belongs to).

## Key lookup

 1. [Compute hash code](4-hashing-algorithms.md) of the key and determine the segment index and the
 part of the key's hash code to be stored in hashLookup using [`hashSplitting`](
 3_1-header-fields.md#hashsplitting) algorithm.

 2. Ensure the lock (the 1st field of the [segment header structure](
 3-memory-layout.md#segment-header-structure) of the segment with the index, determined on the
 previous step (below this segment is simply called the *current* segment), is acquired at least on
 the read level (using one of [read locking operations](
 3_2-lock-structure.md#try-acquire-read-lock).

 This step (and the previous step) might be done already, if the key lookup is performed as a part
 of some multi-key query. Also, if multiple keys in the multi-key query go to the same segment, the
 segment lock is acquired only once at the highest level, required by sub-operations with those
 keys.

 3. Compute the starting slot index in [hash lookups](3-memory-layout.md#hash-lookup), that is the
 reminder of integral division of the [hash lookup key](3-memory-layout.md#hash-lookup-key)
 (computed on the 1st step of this operation) by the [`tierHashLookupCapacity`](
 3_1-header-fields.md#tierhashlookupcapacity).

 4. Read the value in the slot with the index, computed on the previous step, of the current tier
 (initially the [first tier](3-memory-layout.md#main-segments-area) of the current segment).

 This read operation is followed by a read barrier, as formally described in [memory ordering](
 1-design-goals.md#memory-ordering) section of the Chronicle Map's platform assumptions. I. e.
 writes performed before writing the slot value which was read are visible after the read.

 > In the reference Java implementation, this is a volatile read of `int` or `long` value.

 > The memory ordering is needed because [entry insertion](#entry-insertion) is performed with the
 > segment lock held on the update level, then concurrent readers may exist who should read correct
 > entry data after it is inserted by updating the slot value.

 5. If the slot is non-empty, check if the hash lookup key stored in the slot is equal to the hash
 lookup key, computed on the 3rd step. If they are equal, compare the queried key (of this key
 lookup operation) with the key, stored in the [entry](3-memory-layout.md#stored-entry-structure),
 starting from the chunk with the index, stored in the value of the current slot. If the keys are
 equal, the lookup operation is successful (the value could be read from the located [entry
 structure](3-memory-layout.md#stored-entry-structure)).

 After reading the value stored for the queried key (if needed) the segment lock is released,
 however this could be deferred in the case of multi-key query.

 If the slot is empty, continue with the 7th step.

 If any of two checks is failed during this step, continue with the next step.

 6. Compute the next hash lookup slot, by incrementing the current slot index and wrapping around
 [`tierHashLookupCapacity`](3_1-header-fields.md#tierhashlookupcapacity) (i. e. instead of
 `tierHashLookupCapacity` the slot index becomes 0). Then continue with the 4th step.

 7. If the current tier is the last tier in the chain for the current segment, i. e. the value of
 the [next tier field](#next-tier-field) of the current tier is 0, the lookup operation is failed.
 Otherwise, continue with the next linked tier (as the new current tier) and go to the 3rd step.

> The reference Java implementation: [`SegmentStages`](
> ../src/main/java/net/openhft/chronicle/hash/impl/stage/entry/SegmentStages.java), [`KeySearch`](
> ../src/main/java/net/openhft/chronicle/hash/impl/stage/query/KeySearch.java), [`HashQuery`](
> ../src/main/java/net/openhft/chronicle/hash/impl/stage/query/HashQuery.java) classes.

## Entry insertion

 1. Perform a [key lookup](#key-lookup) for the key that is going to be inserted, ensuring that the
 segment is locked at least on the *update* level on the 2nd step of lookup operation. If the
 segment for the key is already locked on the read level (which couldn't be upgraded to a higher
 level), the lock is released, then acquired again on the update level, and then key lookup is
 performed.

 Insertion is possible only if the lookup operation is *failed*.

 2. Compute the number of bytes would be spanned by the [stored entry structure](
 3-memory-layout.md#stored-entry-structure) with the key and the value that are going to be
 inserted, assuming the [worst possible value alignment](3_1-header-fields.md#worstalignment).

 3. Compute the number of chunks that covers the entry size computed on the previous step. I. e. the
 smallest multiple of [`chunkSize`](3_1-header-fields.md#chunksize) that is equal or greater than
 the computed entry size.

 4. Set the *current tier* to the first tier in the segment.

 5. If the value in the [entries number field](#entries-number-field) of the current tier is equal
 to or greater than the [`maxEntriesPerHashLookup`](3_1-header-fields.md#maxentriesperhashlookup),
 go to the 14th step.

 6. Try to find a continuous block of bits set to 0, the number of bits is computed on the 3rd step,
 in the [free list](3-memory-layout.md#free-list) of the current tier.

 Start scanning free list from the index, stored in the [smallest free chunk index
 field](#smallest-free-chunk-index-field) of the current tier.

 If a block of bits set to 0 of the required length is found, continue with the next step, otherwise
 go to the 14th step.

 7. Set the bits, found on the previous step, to 1.

 8. Increment the value in the [entries number field](#entries-number-field) of the current tier.

 9. If the index of the first bit in the block, found on the previous step, equals to the value,
 stored in the [smallest free chunk index field](#smallest-free-chunk-index-field), then update the
 stored value to the index of the bit, following after the found block (or the
 [`actualChunksPerSegmentTier`](3_1-header-fields.md#actualchunkspersegmenttier) value, if the found
 block ends at the end of the free list).

 10. Write the entry to be inserted in the [entry space](3-memory-layout.md#entry-space) of the
 current tier, starting from the chunk at index equal to the index of the first bit in the block,
 that was found and set to 1 on the previous steps. The procedure of writing an entry is effectively
 defined by [stored entry structure](3-memory-layout.md#stored-entry-structure).

 > In the reference Java implementation, writing the checksum is delayed until the segment lock
 > is released or downgraded to the read level. This is done because sometimes (especially when
 > the map is replicated) the entry is updated several times during one operation, delaying checksum
 > write allows to avoid unnecessary multiple checksum computations (relatively expensive).

 11. If the actual value alignment, required by the stored entry and actual addresses at which it
 was stored is smaller than the worst possible alignment, used to compute expected size of entry
 structure on the 2nd step of this operation, and smaller number of chunks is required to cover the
 actual stored entry structure, [free extra chunks](#free-chunks).

 12. [Locate an empty slot](#locate-empty-slot) in the [hash lookup](3-memory-layout.md#hash-lookup)
 of the current tier.

 Instead of re-computing empty slot positions, empty slot indexes could be cached when performed key
 lookup (in the 1st step of entry insertion operation), and used in the next step right away.

 13. Write a new value to the slot, located on the previous step. The value is combined from
 [hash lookup key](3-memory-layout.md#hash-lookup-key) (computed on the 1st step of this operation)
 in the [`tierHashLookupKeyBits`](3_1-header-fields.md#tierhashlookupkeybits) lowest bits and index
 of the first bit found on the 6th step of this operation in the [`tierHashLookupValueBits`](
 3_1-header-fields.md#tierhashlookupvaluebits) higher bits.

 This write operation is preceded by a write barrier, as formally described in [memory ordering](
 1-design-goals.md#memory-ordering) section of the Chronicle Map's platform assumptions. I. e.
 writes performed before the slot value write are visible after this value is read during [key
 lookup](#key-lookup) operation on the 4th step.

 > In the reference Java implementation, this is a volatile write of `int` or `long` value.

 After the 13th step the insertion operation is successful. The segment lock is released, however
 this could be deferred in the case of multi-key query, or the lock could be downgraded to the read
 level.

 14. If the current tier is the last tier in the chain for the segment, i. e. the value of the [next
 tier field](#next-tier-field) of the current tier is 0, go to the 15th step. Otherwise, continue
 with the next linked tier (as the new current tier) and go to the 5th step.

 15. Try to [allocate a new tier](5-initialization.md#tier-allocation). If the allocation operation
 fails, the entry insertion operation also fails. If the allocation is successful, [link the newly
 allocated tier](#link-new-tier-to-segment-chain) to the segment chain.

 Set the current tier to the newly allocated tier and continue with the 5th step.

> The reference Java implementation: [`MapAbsent.putEntry()`](
> ../src/main/java/net/openhft/chronicle/map/impl/stage/query/MapAbsent.java) method.

## Value update

Steps 4-5 are *in-place* value update. Steps from 6 to the end of the operation are called
*relocation* (relocating value update).

 1. Perform a [key lookup](#key-lookup) for the key that value is going to be updated for, ensuring
 that the segment is locked at least on the *update* level on the 2nd step of lookup operation. If
 the segment for the key is already locked on the read level (which couldn't be upgraded to a higher
 level), the lock is released, then acquired again on the update level, and then key lookup is
 performed.

 > Value update always requires the segment lock to be held on the write level at some point of the
 > operation, therefore if the segment is not yet locked on the update level, acquiring the lock
 > on the write level right away allows not to perform a lock upgrade operation later. On the other
 > hand, if there are frequent concurrent readers, reducing the time when the lock is held on an
 > exclusive level improves concurrency. This is an implementation choice. The reference Java
 > implementation takes the second way, i. e. acquires lock on the update level first, and upgrades
 > to the write level for the least possible time.

 Value update is possible only if the lookup operation is *successful*.

 2. Compute the number of bytes of the [entry structure](3-memory-layout.md#stored-entry-structure)
 at the current addresses, but with the updated value. (Alignment could change, because if the value
 size is different, number of bytes taken by value size encoding (the 3rd field of entry structure)
 could also change.)

 3. Compute the new number of chunks that covers the new entry structure.

 If the new number of chunks required for the entry is less than the former, [free the extra
 chunks](#free-chunks). If the new number of chunks is greater than the former, and the block
 of bits of length equal to the number of additional chunks required, and starting at the index of
 chunk immediately following the last chunk taken by the entry currently, is all set to 0, set these
 bits to 1. If the block is not empty (some bits are set to 1), go to the 6th step. Otherwise,
 continue with the next step.

 4. If the segment is locked on the update level, [upgrade to the write
 level](#time-limited-update-to-write-lock-upgrade).

 > In-place value update (the following step) is performed on fully exclusive locking level, because
 > concurrent readers could see inconsistent value state, while the update is in progress. If the
 > value could be updated atomically on machine level: e. g. the value is just 4 or 8 bytes long
 > and could be updated by a single ordinary (atomic on x86) memory transfer, implementations
 > may omit acquiring the segment lock on the write level on this step.

 > Similar effect could be achieved with hardware memory transactions (`XBEGIN` and `XCOMMIT`
 > instructions) when updating larger values, though this topic is not explored in this
 > specification.

 5. Write the new value into the old [entry structure](3-memory-layout.md#stored-entry-structure),
 updating value size if needed. Update the entry checksum (the 6th field of entry structure).

 > In the reference Java implementation, updating the checksum is delayed until the segment lock
 > is released or downgraded to the read level. This is done because sometimes (especially when
 > the map is replicated) the entry is updated several times during one operation, delaying checksum
 > write allows to avoid unnecessary multiple checksum computations (relatively expensive).

 After the 5th step the update operation is successful. The segment lock is released, however this
 could be deferred in the case of multi-key query, or the lock could be downgraded to a lower level.

 6. Compute the number of bytes would be spanned by a new [entry structure](
 3-memory-layout.md#stored-entry-structure) with the queried key and the value that is going to
 replace the previous value, assuming the [worst possible value alignment](
 3_1-header-fields.md#worstalignment).

 7. Compute the number of chunks that covers the entry size computed on the previous step. I. e. the
 smallest multiple of [`chunkSize`](3_1-header-fields.md#chunksize) that is equal or greater than
 the computed entry size.

 8. If the value in the [entries number field](#entries-number-field) of the current tier is equal
 to or greater than the [`maxEntriesPerHashLookup`](3_1-header-fields.md#maxentriesperhashlookup),
 go to the 19th step.

 9. Try to find a continuous block of bits set to 0, the number of bits is computed on the 7th step,
 in the [free list](3-memory-layout.md#free-list) of the current tier.

 Start scanning free list from the index, stored in the [smallest free chunk index
 field](#smallest-free-chunk-index-field) of the current tier.

 If a block of bits set to 0 of the required length is found, continue with the next step, otherwise
 go to the 19th step.

 10. Set the bits, found on the previous step, to 1.

 11. Increment the value in the [entries number field](#entries-number-field) of the current tier.

 12. If the index of the first bit in the block, found on the previous step, equals to the value,
 stored in the [smallest free chunk index field](#smallest-free-chunk-index-field), then update the
 stored value to the index of the bit, following after the found block (or the
 [`actualChunksPerSegmentTier`](3_1-header-fields.md#actualchunkspersegmenttier) value, if the found
 block ends at the end of the free list).

 13. If the current tier is not changed since the 3rd step of this operation, [free the
 chunks](#free-chunks), taken by the previous entry for the queried key.

 > The previous entry if freed only after the new is allocated in order to prohibit overlapping
 > of these blocks of chunks. This is needed, because in the reference Java implementation, the
 > following step is essentially a `sun.misc.Unsafe.copyMemory()` call, that is not guaranteed
 > to work correctly for overlapping memory regions.

 14. Copy the part of the already existing entry (located on the first step of this operation)
 before the 3rd field, i. e. stored key size and the key itself, to the entry space of the current
 tier, starting from the chunk at index equal to the index of the first bit in the block, that was
 found and set to 1 on the previous steps. Write the rest of the entry (the new value size, the
 new value and optionally checksum) after the copied part. The procedure of writing an entry is
 effectively defined by [stored entry structure](3-memory-layout.md#stored-entry-structure).

 > In the reference Java implementation, writing the checksum is delayed until the segment lock
 > is released or downgraded to the read level. This is done because sometimes (especially when
 > the map is replicated) the entry is updated several times during one operation, delaying checksum
 > write allows to avoid unnecessary multiple checksum computations (relatively expensive).

 15. If the actual value alignment, required by the stored entry and actual addresses at which it
 was stored is smaller than the worst possible alignment, used to compute expected size of entry
 structure on the 6th step of this operation, and smaller number of chunks is required to cover the
 actual stored entry structure, [free extra chunks](#free-chunks).

 16. If the current tier is not changed since the 3rd step of this operation, write a new value to
 the slot, located during key lookup on the 1st step of this operation.

 If the current tier is changed since the 3rd step of this operation, [locate an empty
 slot](#locate-empty-slot) in the [hash lookup](3-memory-layout.md#hash-lookup) of the current tier
 and write a new value to this slot.

 The new value is combined from [hash lookup key](3-memory-layout.md#hash-lookup-key) (computed on
 the 1st step of this operation, and remaining unchanged in the slot value) in the
 [`tierHashLookupKeyBits`](3_1-header-fields.md#tierhashlookupkeybits) lowest bits and index of the
 first bit found on the 9th step of this operation in the [`tierHashLookupValueBits`](
 3_1-header-fields.md#tierhashlookupvaluebits) higher bits.

 This write operation is preceded by a write barrier, as formally described in [memory ordering](
 1-design-goals.md#memory-ordering) section of the Chronicle Map's platform assumptions. I. e.
 writes performed before the slot value write are visible after this value is read during [key
 lookup](#key-lookup) operation on the 4th step.

 > In the reference Java implementation, this is a volatile write of `int` or `long` value.

 17. Ensure the segment lock is acquired on the write level. To this step, the segment lock is
 already acquired at least on the update level; [upgrade to the write level](
 3_2-lock-structure.md#time-limited-update-to-write-lock-upgrade) if needed.

 > While relocating value update is in progress, and if a block of free chunks for the updated entry
 > is found in the same tier, where the entry was originally located, concurrent segment readers are
 > safe, because the slot state is updated atomically (on the previous step) and with memory
 > barriers. Acquiring the segment lock on the fully exclusive write level after the relocation is
 > already done, is effectively waiting until all concurrent readers release the segment lock. This
 > is needed, because if some concurrent readers hold the lock (and observe the previous value
 > mapped for the queried key, in the previous location) after value update operation exits and the
 > current thread releases the segment lock, another updating thread might come in and overwrite
 > bytes in the previous entry location in the entry state, compromising long-staying readers. That
 > is why we need to ensure, that the readers which come before this value update operation started
 > (or in the process of the value update) exit before this value update operation exits.
 >
 > Example: [`TrickyContextCasesTest.testPutShouldBeWriteLocked()`](
 > ../src/test/java/net/openhft/chronicle/map/TrickyContextCasesTest.java) method.
 >
 > Possibility to optimize this is explored in [HCOLL-425](
 > https://higherfrequencytrading.atlassian.net/browse/HCOLL-425).
 >
 > If a block of free chunks for the updated entry is found in a different tier from the one where
 > the entry was originally located, the slot should be removed in the previous tier hash lookup
 > (see the next step), that is not safe for concurrent readers, that is why the lock should also be
 > upgraded to the write level.

 18. If the current tier is changed since the 3rd step of this operation, [shift-remove the
 slot](#shift-remove-hash-lookup-slot), located on the 1st step of this operation.

 19. If the current tier is not changed since the 3rd step of this operation, [free the
 chunks](#free-chunks), taken by the previous entry for the queried key.

 20. If this step is performed for the first time, set the current tier to the first tier of this
 segment.

 21. If the current tier is the last tier in the chain for the segment, i. e. the value of the [next
 tier field](#next-tier-field) of the current tier is 0, go to the 22th step. Otherwise, set the
 current tier to the next linked tier. If the current tier equals to the then-current tier on the
 3rd step of this operation, repeat this step. Otherwise, go to the 8th step.

 22. Try to [allocate a new tier](5-initialization.md#tier-allocation). If the allocation operation
 fails, the entry insertion operation also fails. If the allocation is successful, [link the newly
 allocated tier](#link-new-tier-to-segment-chain) to the segment chain.

 Set the current tier to the newly allocated tier and continue with the 8th step.

> The reference Java implementation: mostly [`MapEntryStages.innerDefaultReplaceValue()`](
> ../src/main/java/net/openhft/chronicle/map/impl/stage/entry/MapEntryStages.java) method.

## Entry removal

 1. Perform a [key lookup](#key-lookup) for the key that is going to be removed, ensuring that the
 segment is locked on the *write* level on the 2nd step of lookup operation. If the segment for the
 key is already locked on the read level (which couldn't be upgraded to the write level), the lock
 is released, then acquired again on the write level, and then key lookup is performed.

 Entry removal is possible only if the lookup operation is *successful*.

 2. [Free the chunks](#free-chunks), spanned by the entry.

 3. [Shift-remove the slot in the hash lookup](#shift-remove-hash-lookup-slot), located on the first
 step of this operation.

 > The reference Java implementation: [`HashQuery.doRemove()`](
 > ../src/main/java/net/openhft/chronicle/hash/impl/stage/query/HashQuery.java) method.

Supplementary operations:

#### Locate empty slot

Locate an empty slot in the [hash lookup](3-memory-layout.md#hash-lookup) of some tier, while
querying for some key:

 1. Compute the starting slot index, that is the reminder of integral division of the [hash lookup
 key](3-memory-layout.md#hash-lookup-key) by the [`tierHashLookupCapacity`](
 3_1-header-fields.md#tierhashlookupcapacity).
 2. Read the value in the current slot.
 3. If the slot is empty this sub-operation is finished. Otherwise, continue with the next step.
 4. Compute the next hash lookup slot, by incrementing the current slot index and wrapping around
 [`tierHashLookupCapacity`](3_1-header-fields.md#tierhashlookupcapacity) (i. e. instead of
 `tierHashLookupCapacity` the slot index becomes 0). Then continue with the 2nd step.

> In the reference Java implementation, this sub-operation and the steps 3-6 of [key
> lookup](#key-lookup) operation are implemented by a single `initKeySearch()` method in
> [`KeySearch`](../src/main/java/net/openhft/chronicle/hash/impl/stage/query/KeySearch.java). The
> above sub-operation is weaker than the corresponding part of key lookup: doesn't require volatile
> read of slots values and checking slot contents, only if slot is empty or full.

#### Free chunks

Set to 0 a block of bits with indexes equal to indexes of some block of chunks in the entry space.
If the index of the first cleared bit is less than the value, stored in the [smallest free chunk
index field](#smallest-free-chunk-index-field) of the current tier, set the value of this field to
the index of the first freed extra chunk.

> The reference Java implementation: `free()` and `freeExtra()` methods in [`SegmentStages`](
> ../src/main/java/net/openhft/chronicle/hash/impl/stage/entry/SegmentStages.java) class.

#### Shift-remove hash lookup slot

Removal of some slot in the hash lookup of some tier (an open addressing hash table with linear
probing).

 1. Set the *remove slot* and the *shift slot* to the slot that is going to be removed.
 2. Compute the next shift slot, by incrementing the current shift slot index and wrapping around
 [`tierHashLookupCapacity`](3_1-header-fields.md#tierhashlookupcapacity) (i. e. instead of
 `tierHashLookupCapacity` the slot index becomes 0).
 3. Read the value in the shift slot.
 4. If the shift slot value is 0 (i. e. the slot is empty), go to the 6th step. Otherwise, extract
 the hash lookup key of the shift slot (at the lowest [`tierHashLookupKeyBits`](
 3_1-header-fields.md#tierhashlookupkeybits) bits of the slot value), and determine, if the remove
 slot is on the shortest slot chain between the starting search slot for the extracted hash lookup
 key and the shift slot. In other words, check if the value in the shift slot could be inserted into
 the remove slot (and the hash table is still correct), given that the hash table is not full. If
 so, write the shift slot value to the remove slot, set the remove slot to the current shift slot.
 5. Go to the 2nd step.
 6. Write 0 to the remove slot, i. e. clear the slot.

> The reference Java implementation: [`CompactOffHeapLinearHashTable.remove()`](
> ../src/main/java/net/openhft/chronicle/hash/impl/CompactOffHeapLinearHashTable.java)

#### Link new tier to segment chain

Link a newly-[allocated](5-initialization.md#tier-allocation) tier to the end of the tier chain of
some segment.

 1. Write the [index](3-memory-layout.md#tier-index) of the newly allocated tier to the
 [next tier field](#next-tier-field) of the last tier in the chain.
 2. Write the index of the segment to the 4th field of the [counters area](
 3-memory-layout.md#segment-tier-counters-area) of the newly allocated tier.
 3. Write the length of the tier chain in the segment (before this operation) to the 5th field of
 the counters area of the newly allocated tier.
 4. Write 0 to the 1st field, and the index of the current tier to the 2nd field of the counters
 area of the newly allocated tier.

> The reference Java implementation: [`SegmentStages.nextTier()`](
> ../src/main/java/net/openhft/chronicle/hash/impl/stage/entry/SegmentStages.java) method.

