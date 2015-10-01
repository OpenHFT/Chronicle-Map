# Chronicle Map Design Goals, Assumptions and Guarantees

 - Chronicle Map is an in-memory, highly concurrent key-value store. Generally keys and values are
 arbitrary sequences of bytes.
 - Chronicle Map optionally persists to a *single file* via the memory-mapping facility, present in
 Windows and POSIX-compatible operating systems (`mmap`). The file shouldn't necessarily materialize
 on disk. It's OK for Chronicle Map if the file resides a memory-mounted FS. The whole Chronicle Map
 state is contained in the file contents i. e. it is possible to move, copy or send the file to
 another machine, access it and observe exactly the same Chronicle Map state. Chronicle Map doesn't
 use the file metadata.
 - Chronicle Map supports fully-featured concurrent access from multiple processes, mapping the same
 file.
 - Chronicle Map supports *isolated* multi-key operations, either on the single key-value domain
 (a single Chronicle Map) or several ones. But the multi-key updates to persisted Chronicle Map(s)
 are *not* atomic (basically isolated and atomic in ACID sense here; see the detailed definition
 below in [Guarantees](#Guarantees) section).

 - Chronicle Map runs only on little-endian architectures.
 - Chronicle Map is *not* distributed: the whole data structure is placed in memory of the single
 machine (in a single file, if persisted).

## Guarantees

### Assumptions

#### CPU

 - CPU should support atomic 32-bit compare-and-swap operations with aligned memory, i. e. if
 several threads, possibly belonging to different processes, try to perform compare-and-swap
 operation on the same 4-byte block, one thread succeeds and all the rest fail.
 - Aligned 32-bit or 64-bit writes should be atomic, that means either all 4(8) bytes are written to
 memory, or nothing is written.

The two above points are true for CPUs with x86 and x86_64 architectures.

 - If some values X<sub>i</sub> are written at addresses A<sub>i</sub>, then 32- or 64-bit value Y
 is written at address B, there should be a way to ensure some memory order. In particular, if a
 concurrent thread reads value Y from address B, then it should read X<sub>i</sub values from
 A<sub>i</sub> addresses (but not the old values, stored at these addresses).

 > In Java Memory model terms, this assumption means "writes to A<sub>i</sub> addresses
 > *happens-before* the write of Y value at address B, which *synchronizes-with* read of this value
 > from a concurrent thread". E. g. in Java there are some trivial ways to ensure this memory
 > ordering: volatile write or compare-and-swap at address B with volatile reads of value Y
 > from B in a concurrent thread.

#### Memory-mapping

 - The memory-mapping implementation shouldn't corrupt the mapped file, even if operating system
 execution was interrupted in any way (black out, virtual machine crash, etc.) It means Chronicle
 Map expects some memory might be "stale", i. e. the bits written shortly before operating system
 failure might not be persisted, but it doesn't expect to read the bit values that have never been
 written to the memory.
 - Writes to disk should be at least 4 or 8 bytes atomic, i. e. either aligned 4- or 8-byte blocks
 are fully written to the disk, or not written at all.

 > Modern non-volatile drives (HDDs and SSDs) support atomic sector writes, which are usually at
 > least 512 bytes in size.

On the other hand, it doesn't assume anything specific about how memory if flushed to disk:

 - The order in which memory is flushed (up or down by addresses, random, concurrent, etc.)
 - The maximum timeout dirty memory could not be flushed to disk. i.e. it assumes some memory could
 remain dirty forever.
 - The maximum fraction of dirty memory not written to disk i.e. it assumes that all mapped memory
 could be dirty.

### Guarantees

If the above assumptions are met, Chronicle Map aims to satisfy the following guarantee:

 - Single-key and multi-key accesses and updates to Chronicle Map (multi-key updates could span
 several Chronicle Maps) are concurrently isolated, i. e. accesses involving a certain key are
 totally ordered across accessing threads and processes. All updates made to the entry during the
 previous update are visible during the subsequent access. After a multi-key update, a subsequent
 multi-key query involving a subset of the updated keys (and possibly some more keys) views all
 updates made to all entries corresponding to the keys from that subset.
 - Under any conditions (concurrent access, inter-process access) and using any type of access
 (querying the entry by the key or iteration), only entries that were ever stored in the Chronicle
 Map could be observed. Reading corrupted or half written values by some keys, or observing some
 keys which were never stored during the iteration is disallowed.

If Chronicle Map is persisted to a file and the operating system fails to flush all dirty memory to
disk due to a power-off or any other failure, when the file is mapped to memory and accessed again,
it might be needed to perform a special recovery procedure on the store first, which identifies and
purges corrupted entries from the Chronicle Map. Therefore, *some entries updated shortly before the
failure could be lost.*

## Goals

The ultimate goal of Chronicle Map design is efficiency:

 - If the number of entries in Chronicle Map is much greater than the number of accessor hardware
 threads, it should scale well up to the total number of hardware threads present in the system.
 - Chronicle Map should be a low-latency key-value store, meaning that for any particular
 percentile (90%, 99%, 99.9% etc.) the latency of Chronicle Map operations should be the
 best or among the best compared to latencies of other key-value stores filled with the same data
 and providing similar guarantees.
 - Chronicle Map shouldn't severely overuse memory and disk space for any range and distribution of
 key and values sizes. Chronicle Map exhibits best-in-class memory footprint if keys and values are
 small and fixed-sized.

### Assumptions

Chronicle Map targets the goals outlined above under the following assumptions:

 - It runs on AMD64 (aka x86_64) architecture
 - It runs on Linux
 - The total volume of memory, used by Chronicle Map fits the available physical memory. In other
 words, the memory accessed by Chronicle Map should never be evicted to disk.

If these assumptions are not held, Chronicle Map must still operate correctly and providing the same
guarantees, but it shouldn't be expected to be very efficient.

## Non-goals

 - Alphabetical key ordering (sorted keys).
 - Durability. There are ways to make Chronicle Map look like a durable key-value store, e. g.
 performing `msync` after each operation. However, if required, it would be better use a database
 designed for durability.

The following items are non-goals but might become goals in the future, at least under certain
conditions or with additional assumptions:

 - Entry ordering by insertion, update or access times.
 - Hard latency guarantees. e.g. Chronicle Map doesn't specify "all operations complete in less than
 one second." Persistence via memory-mapping implies that any memory write could trigger flushing
 the whole file to disk, that could take long time.
 - Entry expirations. This could be emulated by storing the last update (access) time in the value
 bytes, checking it on each access to Chronicle Map and, for example, running a background thread
 that traverses the Chronicle Map and cleans expired entries. However it is not guaranteed that
 Chronicle Map would perform better than a cache with expirations implemented as core functionality.
