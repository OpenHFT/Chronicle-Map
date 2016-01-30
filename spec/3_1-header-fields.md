# 3.1. Chronicle Map Data Store Header Fields

Header of a persisted Chronicle Map instance is written once, when the instance and the file are
created, and never changed after that. It is stored in [Binary Wire Format
](https://github.com/OpenHFT/RFC/blob/master/Wire-Format-API/Binary/Binary-Wire-Format-API-0.1.md)).
The top-level structure name is `!net.openhft.chronicle.map.VanillaChronicleMap`. It contains the
following fields:

##### `dataFileVersion`

A `String` containing the version of the reference Java implementation, e. g. `"3.5.0-beta"`.

##### `keyClass`

The `Class` of the Chronicle Map's keys, e. g. `!type int32` (corresponds to `java.lang.Integer`).

##### `keySizeMarshaller`

The marshaller for the Chronicle Map's key sizes, an instance of
`net.openhft.chronicle.hash.serialization.SizeMarshaller`, e. g.
```yaml
!net.openhft.chronicle.hash.serialization.impl.ConstantSizeMarshaller {
    constantSize: 4
}
```

> The reference Java implementation itself defines and uses only two types of size marshallers:
> [`ConstantSizeMarshaller`](
> ../src/main/java/net/openhft/chronicle/hash/serialization/impl/ConstantSizeMarshaller.java) (see
> above), that consumes 0 bytes to "read" and "write" some constant size, and
> [`StopBitSizeMarshaller`](
> ../src/main/java/net/openhft/chronicle/hash/serialization/impl/StopBitSizeMarshaller.java), that
> uses [stop bit encoding](
> https://github.com/OpenHFT/RFC/blob/master/Stop-Bit-Encoding/Stop-Bit-Encoding-0.1.md) to read and
> write sizes.

##### `keyReader`

The reader for the Chronicle Map's keys, an instance of
`net.openhft.chronicle.hash.serialization.SizedReader`, e. g.
```yaml
!net.openhft.chronicle.hash.serialization.impl.IntegerMarshaller {
}
```

##### `keyDataAccess`

The data access strategy for the Chronicle Map's keys, an instance of
`net.openhft.chronicle.hash.serialization.DataAccess`, e. g.
```yaml
!net.openhft.chronicle.hash.serialization.impl.NotReusingSizedMarshallableDataAccess {
    tClass: !type int32,
    sizedReader: !net.openhft.chronicle.hash.serialization.impl.IntegerMarshaller {
    },
    sizedWriter: !net.openhft.chronicle.hash.serialization.impl.IntegerMarshaller {
    }
}
```

##### `checksumEntries`

A flag denoting if checksums are computed and stored for the Chronicle Map's entries along with
them. A boolean value, `true` or `false`.

> In the reference Java implementation, the default value is `true` for persisted Chronicle Maps,
> checksums are computed and stored.

##### `actualSegments`

The number of [segments](2-design-overview.md#logic) in this Chronicle Map. A positive signed
32-bit value, e. g. `16`.

##### `hashSplitting`

The algorithm for choosing both the segment for a key based on it's 64-bit hash code, and
the data to play the role of key in the segment tier's [hash lookups](
2-design-overview.md#segment-tier). An instance of `net.openhft.chronicle.hash.impl.HashSplitting`,
e. g.
```yaml
!net.openhft.chronicle.hash.impl.HashSplitting$ForPowerOf2Segments {
    bits: 4
}
```

> The reference Java implementation defines and uses three types of hash splitting algorithms:
>
>  - `net.openhft.chronicle.hash.impl.HashSplitting$ForPowerOf2Segments`, used when the
>  `actualSegments` value is a power of 2 (except 1). Lowest bits of keys' hash codes (specified by
>  the `bits` field) are used to choose the segment, highest 64 &minus; `bits` are used as keys in
>  segment tier's hash lookups.
>  - `net.openhft.chronicle.hash.impl.HashSplitting$ForSingleSegment` is an optimized version of
>  `HashSplitting$ForPowerOf2Segments`, for the case of the single segment in the Chronicle Map
>  (`actualSegments` equals to 1). The "chosen" segment index is 0, and entire key's hash codes are
>  used as key in segment tier's hash lookups.
>  - `net.openhft.chronicle.hash.impl.HashSplitting$ForNonPowerOf2Segments`, used when the
>  `actualSegments` is not a power of 2. It has a field `segments`, that has the same value, as
>  `actualSegments` in the higher-level Chronicle Map configuration. The segment index is determined
>  as `segmentIndex = (keyHashCode & ‭0x7FFFFFFF) % segments‬`, and the highest 33 bits of keys' hash
>  codes are used as keys in segment tier's hash lookups.

##### `chunkSize`

The size in bytes of the allocation unit (*chunk*) of the segment tier's [entry space](
2-design-overview.md#segment-tier). A positive 64-bit value, e. g. `4`.

> Unless both keys and values are constant sized, the reference Java implementation by default
> chooses a chunk size which is a power of 2, so that an average-sized entry spans from 4 to 8
> chunks.

##### `maxChunksPerEntry`

The maximum number of chunks a single entry could span. A positive signed 32-bit value, with upper
bound of the `actualChunksPerSegmentTier` value, e. g. `3303`.

#####`actualChunksPerSegmentTier`

The size of the segment tier's entry space area, in chunks. A positive 64-bit value, e. g. `3303`.

##### `segmentHeaderSize`

The number of bytes between starts of the segment headers. A 32-bit value, greater or equal to 32,
e. g. `192`.

> Segment headers take only 32 bytes, the rest space between segment headers is alignment aimed to
> reduce false sharing. The maximum reasonable `segmentHeaderSize` value for modern Intel CPUs is
> 192, 3 cache lines. CPUs prefetch cache lines in both directions from the accessed one, containing
> the segment header.

##### `tierHashLookupValueBits`

The number of bits of the tier hash lookup's slots, used to store the index of the first chunk
allocated for the Chronicle Map's entry. A 32-bit value in [1, 63] range, e. g. `12`.

> In the reference Java implementation the number of value bits is as little as enough to encode any
> chunk index, i. e.
> log(2, nextPowerOf2([`actualChunksPerSegmentTier`](#actualchunkspersegmenttier)))

##### `tierHashLookupKeyBits`

The number of bits of the tier hash lookup's slots, used to store parts of the Chronicle Map key's
hash codes, playing the role of key in hash lookup tables. The part of hash codes is extracted by
the [`hashSplitting`](#hashsplitting) algorithm. A 32-bit value is [1, 63] range, e. g. `20`.

##### `tierHashLookupSlotSize`

The size of the tier hash lookup's slots, in bytes. A 32-bit value, `4` or `8`. Invariant:
`tierHashLookupKeyBits` + `tierHashLookupValueBits` = `tierHashLookupSlotSize` * 8.

##### `tierHashLookupCapacity`

The number of slots in the hash lookups. A positive 64-bit value, a power of 2, e. g. `2048`.

##### `maxEntriesPerHashLookup`

The maximum number of entries, allowed to be stored in a segment tier, i. e. number of slots taken
in a hash lookup. A positive 64-bit value, e. g. `1638`.

> In the reference Java implementation, `maxEntriesPerHashLookup` is chosen as
> `tierHashLookupCapacity` * 0.8. When linear-probing hash table's load factor exceeds 0.8, chain
> lengths become unreasonably long.

##### `tierHashLookupInnerSize`

The size of hash lookups in bytes. A positive 64-bit value, equals to [`tierHashLookupInnerSize`
](#tierhashlookupinnersize) * [`tierHashLookupSlotSize`](#tierhashlookupslotsize). A positive
64-bit value, e. g. `8192`.

##### `tierHashLookupOuterSize`

The size of hash lookups + alignment, in bytes. A positive 64-bit value, e. g. `8192`.

> In the reference Java implementation, `tierHashLookupOuterSize` equals to the
> [`tierHashLookupInnerSize`](#tierhashlookupinnersize) value, rounded up to the next multiple of 64
> (i. e. a cache line boundary).

##### `tierFreeListInnerSize`

The size segment tiers' *free lists* (indexes of free chunks in entry spaces), in bytes. A positive
64-bit value, e. g. `416`.

> In the reference Java implementation, `tierFreeListInnerSize` equals to
> the [`actualChunksPerSegmentTier`](#actualchunkspersegmenttier) value, rounded up to the next
> multiple of 64, then divided by 8, i. e. the dimension of this value is bytes, and it is a
> multiple of 8.

##### `tierFreeListOuterSize`

The size of free lists + alignment, in bytes. A positive 64-bit value, e. g. `448`.

> In the reference Java implementation, `tierFreeListOuterSize` equals to the
> [`tierFreeListInnerSize`](#tierfreelistinnersize) value, rounded up to the next multiple of 64
> (i. e. a cache line boundary).

##### `tierEntrySpaceInnerSize`

The size of segment tiers' entry spaces, in bytes. A positive 64-bit value, e. g. `13212`.

##### `tierEntrySpaceInnerOffset`

The offset in entry spaces from the start to the offset of the first chunk. A non-negative 32-bit
value, e. g. `0`.

> This offset is non-zero, when both keys and values are constant sized, and the value size is not a
> multiple of the [`alignment`](#alignment), then the first chunk of the entry space is *misaligned*
> in order to make all entries take the same number of bytes.

##### `tierEntrySpaceOuterSize`

The size of segment tiers' entry spaces + alignment, in bytes. A positive 64-bit value, e. g.
`13248`.

> In the reference Java implementation, `tierEntrySpaceOuterSize` equals to the
> [`tierEntrySpaceInnerSize`](#tierentryspaceinnersize) value, rounded up to the next multiple of 64
> (i. e. a cache line boundary).

##### `tierSize`

The size of segment tiers in this Chronicle Map, in bytes. A positive 64-bit value, e. g. `21952`.

> In the reference Java implementation, `tierSize` equals to [`tierHashLookupOuterSize`
> ](#tierhashlookupoutersize) + 64 (*tier counters area* size) + [`tierFreeListOuterSize`
> ](#tierfreelistoutersize) + [`tierEntrySpaceOuterSize`](#tierentryspaceoutersize) + (optionally)
> 64. The `tierSize` is a multiple of 64 (i. e. it spans integral number of cache lines). The
> optional extra cache line is added, when the `tierSize` is too round, and there are too many
> segments, in order to break collisions of tiers' start addresses by L1 cache banks. See e. g.
> [this post](http://danluu.com/3c-conflict/) for more information on this effect.

##### `maxExtraTiers`

The maximum number of extra tiers could be allocated, before the Chronicle Map is full and rejects
new insertion requests. A non-negative 64-bit value, e. g. `16`.

> In the reference Java implementation, `maxExtraTiers` defaults to the [`actualSegments`
> ](#actualsegments). I. e. the Chronicle Map could nearly double the expected size, before it
> throws `IllegalStateExceptions` upon new insertion requests.

##### `tierBulkSizeInBytes`

The size in bytes of a *tier bulk*, a unit of memory allocation, when extra tiers are needed. A
positive 64-bit value, e. g. `43904`.

> In the reference Java implementation, `tierBulkSizeInBytes` equals to
> [`tierBulkInnerOffsetToTiers`](#tierbulkinneroffsettotiers) + [`tiersInBulk`](#tiersinbulk) *
> [`tierSize`](#tiersize).

##### `tierBulkInnerOffsetToTiers`

The offset from tier bulks' starts to the address of the first tier within a bulk, in bytes. This
space is used as metadata of a tier bulk. A non-negative 64-bit value, e. g. `0`.

##### `tiersInBulk`

The number of tiers in the tier bulks. A positive 64-bit value, e. g. `2`.

##### `log2TiersInBulk`

The value of this field equals to log(2, [`tiersInBulk`](#tiersinbulk)).

##### `valueClass`

The `Class` of the Chronicle Map's values, e. g. `!type CharSequence`.

##### `valueSizeMarshaller`

The marshaller for the Chronicle Map's value sizes, an instance of
`net.openhft.chronicle.hash.serialization.SizeMarshaller`, e. g.
```yaml
!net.openhft.chronicle.hash.serialization.impl.StopBitSizeMarshaller {
}
```

> See the comments on [`keySizeMarshaller`](#keysizemarshaller) for information about this field.

##### `valueReader`

The reader for the Chronicle Map's values, an instance of
`net.openhft.chronicle.hash.serialization.SizedReader`, e. g.
```yaml
!net.openhft.chronicle.hash.serialization.impl.CharSequenceSizedReader {
}
```

##### `valueDataAccess`

The data access strategy for the Chronicle Map's values, an instance of
`net.openhft.chronicle.hash.serialization.DataAccess`, e. g.
```yaml
!net.openhft.chronicle.hash.serialization.impl.SizedMarshallableDataAccess {
    tClass: !type CharSequence,
    sizedReader: !net.openhft.chronicle.hash.serialization.impl.CharSequenceSizedReader {
    },
    sizedWriter: !net.openhft.chronicle.hash.serialization.impl.CharSequenceSizedWriter {
    }
}
```

##### `constantlySizedEntry`

A flag denoting if both keys and values of the Chronicle Map are constant sized. A boolean value,
`true` or `false`.

##### `alignment`

The alignment (in bytes) of the values of this Chronicle Maps, by addresses, when residing the entry
spaces. A positive, 32-bit, power of 2 value, e. g. `1`.

##### `worstAlignment`

The most bytes could theoretically be wasted on the value [`alignment`](#alignment). A non-negative
32-bit value, e. g. `0`.

> ## The reference Java implementation
>
> The described fields are defined in [`VanillaChronicleHash`
> ](../src/main/java/net/openhft/chronicle/hash/impl/VanillaChronicleHash.java) and
> [`VanillaChronicleMap`](../src/main/java/net/openhft/chronicle/map/VanillaChronicleMap.java).