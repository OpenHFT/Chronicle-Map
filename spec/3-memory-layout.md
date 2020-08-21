# 3. Chronicle Map Data Store Memory Layout

A newly created Chronicle Map store (or just mapped from an already existing persistence file) takes
one big continuous block of memory. Its structure, from lower addresses to higher:

 1. [The self-bootstrapping header](#self-bootstrapping-header).
 2. The alignment to the next cache line boundary (to the next multiple of 64) by addresses.

 > The purpose of this alignment is to make the *global mutable state* to always fit a single cache
 > line.

 The self-bootstrapping header and the alignment are not present, when the Chronicle Map is not
 persisted to disk.

 3. [The global mutable state](#global-mutable-state).
 4. [Segment header alignment](#segment-headers-alignment")
 The alignment to the next page boundary by addresses (page size of the current memory mapping),
 if a new Chronicle Map is created, if an existing one (i. e. persisted) is loaded, the alignment
 is read from the global mutable state (specifically this field of the global mutable state is
 actually immutable, once written).

 > The purpose of this alignment is to minimize the number of pages spanned by the following
 > *segment headers area*. The segment headers area is frequently accessed and updated, so the pages
 > it spans almost always reside in the TLB cache and always need to be flushed to the disk.

 > The alignment (more precisely, the offset to the next area, the segment headers area) is stored
 > in the global mutable state, rather then computed each time a mapped Chronicle Map store is
 > accessed, because other accessing file systems, operation systems and Chronicle Map
 > implementations may use a different page size for memory mapping.

 5. [The segment headers area](#segment-headers-area).
 6. [The main segments area](#main-segments-area).
 7. Zero or more [extra tier bulks](#extra-tier-bulks).

The process of initialization of the Chronicle Map's memory is described on [Initialization Order](
5-initialization.md) page.

## Self-bootstrapping header

The concept is described in the [Self Bootstrapping Data](
https://github.com/OpenHFT/RFC/blob/master/Self-Bootstrapping-Data/Self-Bootstraping-Data-0.1.md)
specification.

The structure of this area is described in the [Size Prefixed Blob](
https://github.com/OpenHFT/RFC/blob/master/Size-Prefixed-Blob/Size-Prefixed-Blob-0.1.md)
specification. The first 8 bytes contains the hash value of bytes sequence from 9th byte to the end
of this size-prefixed blob, computed by [xxHash](https://github.com/Cyan4973/xxHash/) algorithm
(XXH64 version). The "message" is marked as user data (i. e. the user/meta data bit is set to 0).

The self-bootstrapping header itself (i. e. the "message" of the size-prefixed blob) is encoded in
Text Wire format. Once created, this header is never changed. It contains all configurations,
immutable for Chronicle Map during the data store lifetime: number of segments, various sizes,
offsets, etc. See the specification of the fields on [Map Header Fields](3_1-header-fields.md) page.

## Global mutable state

The global mutable state is 33 bytes long.

 1. Bytes 0..7 - the global mutable state lock, a [lock structure](3_2-lock-structure.md).

 > The lock structure supports three levels of locking, while for the segment header only exclusive
 > (the write lock) level is used. This is done for sharing the lock implementation for the
 > global mutable state and segment headers (see below).

 2. Bytes 8..10 - the number of allocated [extra tier bulks](#extra-tier-bulks). An unsigned 24-bit
 value, stored in little-endian order.

 See also the [extra tier bulk allocation](5-initialization.md#entra-tier-bulk-allocation)
 operation.
 
 3. Bytes 11..15 - the index of the first *free* segment tier. An unsigned 40-bit value, stored
 in little-endian order. Extra segment tiers are allocated in bulks, so there is usually a chain
 of allocated, but unused yet segment tiers. This field points to the head of this chain, or has
 value `0`, if there are no free segment tiers.

 ##### Tier index
 *Tier index* is *1-counted* from the beginning of the main segments area, and counting continues in
 the extra tier bulks, i. e. the first tier of the segment #0 has *tier index* 1, the first tier of
 the segment #1 has tier index 2, ... the first tier of the last segment (it's index is
 [`actualSegments`](3_1-header-fields.md#actualsegments) &minus; 1) has tier index `actualSegments`,
 the first tier of the first extra tier bulk has tier index `actualSegments` + 1, etc. Tier indexes
 are 1-counted, because value 0 has some special meaning.
 
 4. Bytes 16..20 - the number of used extra segment tiers. An unsigned 40-bit value, stored in
 little-endian order.
 
 5. Bytes 21..24 - the offset of the segment headers area from the beginning of the memory of this
 Chronicle Map store. An unsigned 32-bit value, stored in little-endian ordered. This field
 determines the size of [the 4th area of the general Chronicle Map structure](
 #segment-headers-alignment).
 
 6. Bytes 25..32 - the Chronicle Map data store size, the offset to the end of the [main segments
 area](#main-segments area) or the last [extra tier bulk](#extra-tier-bulks). A non-negative 64-bit
 value, stored in little-endian order.

> The reference Java implementation: [`VanillaGlobalMutableState`
> ](../src/main/java/net/openhft/chronicle/hash/VanillaGlobalMutableState.java).

## Segment headers area

The offset to this area is stored in the 5th field of the [global mutable state
](#global-mutable-state).

The size (in bytes) of a segment header's area is [`actualSegments`](
3_1-header-fields.md#actualsegments) * [`segmentHeaderSize`](3_1-header-fields.md#segmentheadersize
). Each segment header starts at offsets from the start of the segment headers area, that are
multiples of `segmentHeaderSize`. Each segment header is 32 bytes long. `segmentHeaderSize` &minus;
32 is the alignment between segment headers.

> The purpose of the alignment between segment headers is reducing false sharing, when adjacent
> segment headers are accessed concurrently.

### Segment header structure

 1. Bytes 0..7 - the segment [lock structure](3_2-lock-structure.md).
 2. Bytes 8..11 - the number of entries, stored in the first tier of the segment. A 32-bit
 *unsigned* value, stored in the little-endian order.
 3. Bytes 12..15 - the smallest index of a chunk in the entry space of the first tier of the
 segment, that could possibly be free. A 32-bit *unsigned* value, stored in the little-endian order.
 This field is used to optimize allocation of space for new entries, the search for a sufficient
 range of continuous free chunks in the [free list](#free-list) is started from this index, rather
 than 0. This field is updated on each entry allocation and deletion in the first tier of the
 segment, if the smallest index of a free chunk is changed. When all chunks in the entry space are
 allocated (i. e. the current tier becomes *full*), the value of this field is changed to
 [`actualChunksPerSegmentTier`](3_1-header-fields.md#actualchunkspersegmenttier) (an impossible
 chunk index, which varies from 0 to `actualChunksPerSegmentTier` &minus; 1).
 4. Bytes 16..23 - the [index](#tier-index) of the next segment tier, chained after the first tier
 of the segment. I. e. this field points to the second tier in the chain for the segment, if any.

 It is a 64-bit value, stored in the little-endian order. If the value of this field is 0, this
 means there is no chained segment tier in this segment yet after the first tier, in other words,
 the first tier is the only one in the chain for the current segment.
 
 5. Bytes 24..31 - reserved for use by extensions.

> The reference Java implementation: [`BigSegmentHeader`
> ](../src/main/java/net/openhft/chronicle/hash/impl/BigSegmentHeader.java)

## Main segments area

This area contains the first tiers of the Chronicle Map's segments.

A main segments area starts immediately after the end of a segment headers area without extra
offsets and alignments.

The size of a main segments area is [`actualSegments`](3_1-header-fields.md#actualsegments) *
[`tierSize`](3_1-header-fields.md#tiersize). Segment tiers in a main segments area don't have
extra gaps in memory between each other.

### Segment tier structure

See also [segment tiers design overview](2-design-overview.md#segment-tier) for more explanations
about this structure.

A segment tier is [`tierSize`](3_1-header-fields.md#tiersize) bytes long.

The segment tier structure:

 1. [Hash lookup](#hash-lookup)
 2. [Segment tier counters area](#segment-tier-counters-area)
 3. [Free list](#free-list)
 4. [Entry space](#entry-space)

#### Hash lookup

A hash lookup area starts at the same address as a segment tier, containing it.

A hash lookup consists of [`tierHashLookupCapacity`](3_1-header-fields.md#tierhashlookupcapacity)
slots each of [`tierHashLookupSlotSize`](3_1-header-fields.md#tierhashlookupslotsize) bytes. In each
slot a 32-bit or 64-bit (if the `tierHashLookupSlotSize` is 4 or 8, respectively) value is stored in
little-endian order. A slot value of 0 designates an empty slot.

##### Hash lookup key

In [`tierHashLookupKeyBits`](3_1-header-fields.md#tierhashlookupkeybits) lower bits of a slot value
a *hash lookup key* is stored. It is a part of a Chronicle Map's key hash code, extracted by the
[`hashSplitting`](3_1-header-fields.md#hashsplitting) algorithm. In addition, if the `hashSplitting`
extracts the part of the hash code of 0, a hash lookup key of all set `tierHashLookupKeyBits` bits
(i. e. `(1 << tierHashLookupKeyBits) - 1`) is used instead, to avoid the full hash lookup slot to
look like an empty slot, if the *hash lookup value* is also 0.

In [`tierHashLookupValueBits`](3_1-header-fields.md#tierhashlookupvaluebits) bits, following after
the lower key bits (i. e. bits from `tierHashLookupKeyBits`-th to `tierHashLookupKeyBits +
tierHashLookupValueBits - 1`-th, inclusive) of a slot value a *hash lookup value* is stored. It is
an index of the first chunk of the range of chunks (possibly only a single chunk) in the entry space
of this segment tier, in which a Chronicle Map's entry is stored.

> The reference Java implementation: [`CompactOffHeapLinearHashTable`
> ](../src/main/java/net/openhft/chronicle/hash/impl/CompactOffHeapLinearHashTable.java) (the base
> class), [`IntCompactOffHeapLinearHashTable`
> ](../src/main/java/net/openhft/chronicle/hash/impl/IntCompactOffHeapLinearHashTable.java) (the
> subclass for hash lookups with 4-byte slots), [`LongCompactOffHeapLinearHashTable`
> ](../src/main/java/net/openhft/chronicle/hash/impl/LongCompactOffHeapLinearHashTable.java) (the
> subclass for hash lookups with 8-byte slots).

#### Segment tier counters area

A segment tier counters area starts with offset [`tierHashLookupOuterSize`
](3_1-header-fields.md#tierhashlookupoutersize) from the beginning address of a containing segment
tier.

The segment tier counters structure is 64 bytes long:

 1. Bytes 0..7 - for tiers from extra tier bulks (i. e. tiers that are not first in chains of
 their segments), this field is the [index](#tier-index) of the next segment tier, chained after
 this segment tier. I. e. this field value in the second tier in the chain - to the third tier in
 the chain, and so on. For tiers from the main segment area (i. e. first in chains of their
 segments), this field is unused, the 4th field of the [segment header
 structure](#segment-header-structure) is used instead. This field and the 4th field in the segment
 header structure have the same semantics, with the only difference that this field serves chained
 tiers (tiers from extra tier bulks), while the 4th field in the segment header structure serves
 first tiers in chains of their segments.

 It is a 64-bit value, stored in the little-endian order. If the value of this field is 0, this
 means there is no chained segment tier in this segment yet after the current tier, in other words,
 the current tier is the last in the chain for the current segment.

 When the tier is in the *free* state, i. e. allocated in the extra tier bulk, but not yet assigned
 to some segment, the value of this field is the index of the next *free* tier. The index of the
 first free tier is pointed by the 3rd field of the [global mutable state](#global-mutable-state).

 > This field is effectively "pulled" from the tier counters structure to the segment header
 > structure for tiers in the main segment area, because if there is only one tier in the chain
 > (that should be true for the majority of (or all) segments), the tier counters area is not
 > touched, reducing the number of accessed cache lines and pollution of caches.

 2. Bytes 8..15 - the [index](#tier-index) of the previous segment tier, chained before this segment
 tier. It is a 64-bit value, stored in the little-endian order. In tiers in the main segments area,
 i. e. first tiers in segments' chains, this field has value 0, this means there is no previous
 tiers in chains.

 This field together with the previous field form a doubly-linked list of chained tiers within each
 segment.

 3. Bytes 16..23 - for tiers from extra tier bulks (i. e. tiers that are not first in chains of
 their segments), this field is the smallest index of a chunk in the entry space of this tier,
 that could possibly be free. For tiers from the main segment area (i. e. first in chains of their
 segments), this field is unused, the 3rd field of the [segment header structure
 ](#segment-header-structure) is used instead. This field and the 3rd field in the segment header
 structure have the same semantics, with the only difference that this field serves chained tiers
 (tiers from extra tier bulks), while the 3rd field in the segment header structure serves first
 tiers in chains of their segments.

 A 64-bit non-negative value, stored in the little-endian order.

 > Like the 1st field of this structure, this field is "pulled" to the segment headers in order to
 > avoid accessing tier counters area altogether, when there is only one tier in a segment chain.

 4. Bytes 24..27 - the index of the segment, to which this tier belongs. A 32-bit non-negative
 value, stored in the little-endian order.

 5. Bytes 28..31 - the 0-counted order of this tier in the chain of the segment. A 32-bit
 non-negative value, stored in the little-endian order. In the tiers from the main segments area the
 value of this field is 0 (because they are first in chains of their segments), in the tiers which
 are second in chains of their segments the value of this field is 1, in third tiers - 2, and so on.

 6. Bytes 32..35 - for tiers that from extra tier bulks (i. e. not first in chains of their segments
 ), this field is the number of entries, stored in the tier. In tiers from the main segments area
 this field is unused, the 2nd field of the [segment header structure](#segment-header-structure) is
 used instead. Like the 3rd field of the tier counters structure, this field and the 2nd field in
 the segment header structure have the same semantics, with the only difference that this field
 serves chained tiers (tiers from extra tier bulks), while the 3rd field in the segment header
 structure serves first tiers in chains of their segments.

 A 32-bit unsigned value, stored in the little-endian order.

 > Like the 1st and 3rd field of this structure, this field is "pulled" to the segment headers in
 > order to avoid accessing tier counters area altogether, when there is only one tier in a segment
 > chain.

 7. Bytes 36..63 - reserved for use by extensions.

> The reference Java implementation: [`TierCountersArea`
> ](../src/main/java/net/openhft/chronicle/hash/impl/TierCountersArea.java).

#### Free list

A free list starts with offset [`tierHashLookupOuterSize`](
3_1-header-fields.md#tierhashlookupoutersize) + 64 from the beginning address of a containing
segment tier.

This is a list of [`actualChunksPerSegmentTier`](3_1-header-fields.md#actualchunkspersegmenttier)
bits, each corresponding to a chunk in the [entry space](#entry-space). If the chunk is allocated
for some entry, the corresponding bit is set to 1, otherwise it is clear.

> The reference Java implementation: [`SingleThreadedFlatBitSetFrame`](
> https://github.com/OpenHFT/Chronicle-Algorithms/blob/chronicle-algorithms-1.1.6/src/main/java/net/openhft/chronicle/algo/bitset/SingleThreadedFlatBitSetFrame.java).

#### Entry space

An entry space starts with offset [`tierHashLookupOuterSize`
](3_1-header-fields.md#tierhashlookupoutersize) + 64 + [`tierFreeListOuterSize`
](3_1-header-fields.md#tierfreelistoutersize) from the beginning address of a containing segment
tier.

The entry space starts with an internal offset of [`tierEntrySpaceInnerOffset`
](3_1-header-fields.md#tierentryspaceinneroffset) bytes. Then [`actualChunksPerSegmentTier`
](3_1-header-fields.md#actualchunkspersegmenttier) *chunks* follow without gaps between each other,
each chunk of [`chunkSize`](3_1-header-fields.md#chunksize) bytes.

A chunk is the minimum allocation unit. A single or several continuous chunks are used to store the
Chronicle Map's entries. These ranges doesn't intersect, i. e. each chunk is used to store at most
one entry.

##### Stored entry structure

Within the given range of chunks, the entry is stored starting from the beginning of the first chunk
of the range. The stored entry structure is:

 1. The length of the key (in bytes), stored using the [`keySizeMarshaller`
 ](3_1-header-fields.md#keysizemarshaller) algorithm.
 2. The key itself stored, a sequence of bytes of the length, determined by the previous field of
 this structure. The key is stored using the [`keyDataAccess`](3_1-header-fields.md#keydataaccess)
 strategy.
 3. The length of the value (in bytes), stored using the [`valueSizeMarshaller`
 ](3_1-header-fields.md#valuesizemarshaller) algorithm.
 4. The gap (likely 0-long, though), imposed by the value [`alignment`
 ](3_1-header-fields.md#alignment) strategy.
 5. The value itself stored, a sequence of bytes of the length, determined by the 3rd field of this
 structure. The value is stored using the [`valueDataAccess`](3_1-header-fields.md#valuedataaccess)
 strategy.
 6. (Optional field) If [`checksumEntries`](3_1-header-fields.md#checksumentries) is `true`, there
 is a 4-byte checksum stored after the value bytes. See also the [checksum algorithm
 ](4-hashing-algorithms.md#checksum-algorithm.md).

The memory between the end of the 6th field (or 5th, if checksums are not stored) and the end of the
last chunk, allocated for the entry, is unused. This is an internal fragmentation.

> The reference Java implementation of the stored entry structure: primarily [`HashEntryStages`
> ](../src/main/java/net/openhft/chronicle/hash/impl/stage/entry/HashEntryStages.java) and
> [`MapEntryStages`
> ](../src/main/java/net/openhft/chronicle/map/impl/stage/entry/MapEntryStages.java) classes.

## Extra tier bulks

The size of an extra tier bulks area is [`tierBulkSizeInBytes`
](3_1-header-fields.md#tierbulksizeinbytes), multiplied by the number of extra tier bulks, stored
in the 2nd field of the [global mutable state](#global-mutable-state) structure. Extra tier bulks
start immediately at the end of the main segments area (the end of the last segment tier in the main
segments area) and doesn't have gaps between each other.

### Extra tier bulk

Each extra tier bulk starts with metadata space of [`tierBulkInnerOffsetToTiers`
](3_1-header-fields.md#tierbulkinneroffsettotiers) (reserved for use by extensions), after that
[`tiersInBulk`](3_1-header-fields.md#tiersinbulk) segment tiers are located without gaps between
each other.

See the [segment tier structure](#segment-tier-structure) for information about the detailed
structure of each particular tier in extra tier bulks.

