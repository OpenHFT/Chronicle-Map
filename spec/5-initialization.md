# 5. Chronicle Map Data Store Initialization Operations

> In the reference Java implementation the operations, described on this page, are defined in
> [`ChronicleMapBuilder`](../src/main/java/net/openhft/chronicle/map/ChronicleMapBuilder.java) and
> [`VanillaChronicleHash`](
> ..\src\main\java\net\openhft\chronicle\hash\impl\VanillaChronicleHash.java) classes.

## Chronicle Map creation

This section describes the process of creation of a new Chronicle Map data store (or opening the
existing Map, persisted to some file).

 1. If the Chronicle Map should be persisted to some file, and this file does not exist, this file
 is created as empty, i. e. with size of 0 bytes.

 2. If the Chronicle Map should be persisted to some file, and the size of the file is non-zero,
 [wait until the Chronicle Map is ready](#wait-until-chronicle-map-is-ready). Otherwise, acquire
 an exclusive file lock, and check the file size again. If the file size is non-zero now, release
 the file lock and [wait until the Chronicle Map is ready](#wait-until-chronicle-map-is-ready). If
 the file is still empty, [write the self bootstrapping header](#write-self-bootstrapping-header) to the
 beginning of the file, then release the file lock.

 > The purpose of acquiring file lock is to ensure that only one concurrent process (if any)
 > initializes the Chronicle Map data store, others wait until they could open the already created
 > Chronicle Map. This also excludes possibility for any issues with conflicting Chronicle Map
 > configurations, attempted to be created from different processes.

 > This step follows [double-checked locking](https://en.wikipedia.org/wiki/Double-checked_locking)
 > pattern to avoid file locking in most cases when an already existing Chronicle Map is opened.

 This step is serialized for the persistence file among threads within the current OS process,
 because file locking mechanism in operating systems is per-process, not per-thread. I. e.
 concurrent threads within the same OS process don't lock the same file at the same time.

 Concurrent threads perform any initialization operations (Chronicle Map creation and [extra tier
 bulk allocation](#extra-tier-bulk-allocation)) with canonicalized file descriptor, that is closed
 only after last Chronicle Map in-process representation structure, referencing this Chronicle Map
 data store (hence this file descriptor) is disposed. This is required, because in Linux, when any
 file descriptor referencing a file is closed, all file locks are released unconditionally.

 If on this step we were waiting until Chronicle Map is ready, then start using Chronicle Map
 normally. If on this step we wrote self-bootstrapping header or the Chronicle Map is not persisted
 to any file (i. e. purely in-memory), continue initialization with following steps:

 3. Zero out the [global mutable state](3-memory-layout.md#global-mutable-state)'s space in the
 file.
 4. Zero out the [segment headers area](3-memory-layout.md#segment-headers-area) in the file.
 5. For each segment tier in the [main segments area](3-memory-layout.md#main-segments-area), zero
 out memory from the beginning of the tier to the start of the [entry space](
 3-memory-layout.md#entry-space), i. e. zero out this tier's hash lookup, segment tier counters area
 and free list.
 6. Write the segment headers offset into the 5th field of the global mutable state.
 7. Write the offset to the end of the main segments area into the 6th field of the global mutable
 state.
 8. If the Chronicle Map is persisted, ensure all data written to the file is flushed to the disk.
 For example on Linux, this could be done with `msync` and `fdatasync` calls, on Windows - with
 `FlushFileBuffers` system call.
 9. If the Chronicle Map is persisted, write the readiness bit into the header, i. e. the highest
 bit of the 32-bit word by offset 8 from the beginning of the file, read and written in the
 little-endian order. Read the [Size Prefix Blob](
 https://github.com/OpenHFT/RFC/blob/master/Size-Prefixed-Blob/Size-Prefixed-Blob-0.1.md)
 specification for details. This step is the event that is actually awaited by the threads (across
 OS processes) [waiting until Chronicle Map is ready](#wait-until-chronicle-map-is-ready).

### Wait until Chronicle Map is ready

Check until the readiness bit is set to 0. The readiness bit it the highest bit of 32-bit word by
offset 8 from the beginning of the Chronicle Map persistence file, read in the little-endian order.

Optionally yield processor resources after failed checks, e. g. make the waiting thread sleep for
some time. The exact yielding operation (or if it is making the current thread sleeping, the exact
sleep interval) is unspecified.

> The reference Java implementations turns the waiting thread to sleep for 100 ms after failed
> attempts.

Implementations must not wait until the Chronicle Map is ready indefinitely, after some finite
number of failed checks, or some finite time elapsed since the start, the wait procedure must fail.

> Indefinite wait is dead lock prone, if the thread doing the initialization fails itself.

> The reference Java implementation checks until the Chronicle Map is ready for approximately 1
> minute, then throws an exception.

### Write self bootstrapping header

 1. Determine the self-bootstrapping header length (in bytes).
 2. Write the correct hash code (64-bit value) at the beginning 8 bytes of the file, in the
 little-endian order. The "correct hash code" is the result of application of xxHash (XXH64 version)
 algorithm to the bytes sequence, first 4 bytes of which is the self-bootstrapping header length,
 determined on the step 1, encoded in the little-endian order, the rest bytes of the sequence
 is the self-bootstrapping header itself.
 3. Write the length (determined on the step 1) to the size word (by offset 8 from the beginning of
 the file), with the readiness bit set to 1.
 4. Write the self-bootstrapping header to the file itself, starting from offset 12 in the file.

See also [memory layout of self-bootstrapping header](3-memory-layout.md#self-bootstrapping-header).

## Extra tier bulk allocation

This procedure is called only within [tier allocation](#tier-allocation) procedure, hence guarded
by [global mutable state](3-memory-layout.md#global-mutable-state) lock.

 1. If the Chronicle Map is persisted to disk, extend the file's mapping from the current end of the
 Chronicle Map data store (either the end of the [main segments area](
 3-memory-layout.md#main-segments-area) or the end of the previous extra tier bulk) by the minimum
 memory amount allowed for mapping (a multiple of the native page size), that covers space required
 for the next extra tier bulk, i. e. [`tierBulkSizeInBytes`](
 3_1-header-fields.md#tierbulksizeinbytes) bytes from the previous end of the Chronicle Map data
 store.
 2. Prepare the extra tier bulk's metadata space, that starts at the beginning of the extra tier
 bulk and spans [`tierBulkInnerOffsetToTiers`](3_1-header-fields.md#tierbulkinneroffsettotiers),
 according to the way how metadata space is actually used. This is unspecified.
 3. For each tier in the extra tier bulk, zero out memory from the beginning of the tier to the
 start of the [entry space](3-memory-layout.md#entry-space), i. e. zero out this tier's hash lookup,
 segment tier counters area and free list. This step is equivalent to the 5th step of the [Chronicle
 Map creation](#chronicle-map-creation) procedure.
 4. For each (but the last one) tier in the extra tier bulk, write [index of the next tier](
 3-memory-layout.md#tier-index) into the 1st field of the [tier counters area](
 3-memory-layout.md#segment-tier-counters-area). This step forms a singly-linked list of allocated,
 but unused yet segment tiers.
 5. If the Chronicle Map is persisted, ensure that memory of the newly allocated extra tier bulk
 is flushed to the disk (via `msync` call on Linux, `FlushFileBuffers` on Windows).
 6. Increment the value in the 2nd field (the number of allocated extra tier bulks) of the [global
 mutable state](3-memory-layout.md#global-mutable-state) of this Chronicle Map.
 7. Write the [index](3-memory-layout.md#tier-index) of the first tier in this extra tier bulk into
 the 3rd field (the index of the first *free* segment tier) of this Chronicle Map's global mutable
 state.
 8. Increment the value in the 6th field (the data store size) of the global mutable state by
 [`tierBulkSizeInBytes`](3_1-header-fields.md#tierbulksizeinbytes).

## Tier allocation

 1. [Acquire write lock](3_2-lock-structure.md#time-limited-write-lock-acquisition) on the [global
 mutable state](3-memory-layout.md#global-mutable-state) lock (the 1st field). If acquisition is
 not successful, this operation fails.
 2. Check that the value in the 4th field (the number of used extra segment tiers) of the global
 mutable state is less than the [`maxExtraTiers`](3_1-header-fields.md#maxextratiers) value,
 otherwise the operation fails.
 3. Read the value in the 3rd field (the index of the first free segment tier) of the global mutable
 state. If the value is not positive, [allocate an extra tier bulk](#extra-tier-bulk-allocation),
 and then read the value in the 3rd field of the global mutable state again. If it not positive,
 the operation fails.
 4. Read the value of the 1st field (index of the next linked tier) of the tier counter area in the
 tier with index, read on the previous step.
 5. Increment the value in the 4th field (the number of used extra segment tiers) of the global
 mutable state.
 6. Write the tier index, read on the 4th step of this operation, into the 3rd field (the index of
 the first free segment tier) of the global mutable state.
 7. [Release write lock](3_2-lock-structure.md#release-write-lock) on the global mutable state lock.
 If the release is not successful, this operation fails.

The result of the procedure is the tier index, read on the 3rd step.