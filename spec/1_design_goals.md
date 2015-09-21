# Chronicle Map Design Goals, Assumptions and Guarantees

 - Chronicle Map is an in-memory, highly concurrent key-value store. Generally keys and values are
 arbitrary sequences of bytes.
 - Chronicle Map optionally persists to a *single file* via the memory-mapping facility, present in
 Windows and POSIX-compatible operation systems (`mmap`). The file shouldn't necessarily materialize
 on disk. It's OK for Chronicle Map if the file resides a memory-mounted FS. The whole Chronicle Map state
 is contained in the file contents i. e. it is possible to move, copy or send the file to another
 machine, access it and observe exactly the same Chronicle Map state. Chronicle Map doesn't use the file
 metadata.
 - Chronicle Map supports fully-featured concurrent access from multiple processes, mapping the same
 file.
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
 - If some values X<sub>i</sub> are written at addresses A<sub>i</sub>, then 32- or 64-bit value Y
 is written at address B, there should be a way to ensure some memory order. In particular, if a
 concurrent thread reads value Y from address B, then it should read X<sub>i</sub values from
 A<sub>i</sub> addresses (but not the old values, stored at these addresses).

#### Memory-mapping

Chronicle Map assumes that the memory-mapping implementation does not corrupt the mapped file, even
if operation system execution was interrupted in any way (black out, virtual machine crash, etc.)
It means Chronicle Map expects some memory might be "stale", i. e. the bits written shortly before
operation system failure might not be persisted, but it doesn't expect to read the bit values that
have never been written to the memory.

On the other hand, it doesn't assume anything specific about how memory if flushed to disk:

 - The order in which memory is flushed (up or down by addresses, random, concurrent, etc.)
 - The minimum persistence atomicity i.e. Chronicle Map doesn't assume memory blocks of any size
 should be either fully written to disk or not written at all, down to a single bit.
 - The maximum timeout dirty memory could not be flushed to disk. i.e. it assumes some memory could remain
 dirty forever.
 - The maximum fraction of dirty memory not written to disk i.e. it assumes that all mapped memory could be
 dirty.

### Guarantees

If the above assumptions are met, Chronicle Map aims to satisfy the following guarantees:

 - Under any conditions (concurrent access, inter-process access, access after operation system
 failures) and using any type of access (querying the entry by the key or iteration), only entries that
 were ever stored in the Chronicle Map could be observed. Reading corrupted or half written values
 by some keys, or observing some keys which were never stored during the iteration is disallowed.
 - If Chronicle Map is persisted to a file and the operation system fails to flush all dirty memory to
 disk due to a power-off or any other failure, when the file is mapped to memory and accessed again,
 some values associated with the keys shortly before the failure could be lost.
 - If some entry is successfully put into the Chronicle Map, and, if the Chronicle Map is persisted
 to disk, all memory that is dirty by the end of this operation is successfully flushed, this entry
 couldn't disappear from the Chronicle Map unless explicitly removed from the key-value store.

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
 words, the memory accessed by Chronicle Map should never be swapped to disk.

If these assumptions are not held, Chronicle Map must remain operational, but it shouldn't be
expected to be very efficient.

## Non-goals

 - Alphabetical key ordering (sorted keys).
 - Durability. There are ways to make Chronicle Map look like a durable key-value store, e. g.
 performing `msync` after each operation. However, if required, it would be better use a database designed for durability.

The following items are non-goals but might become goals in the future, at least under certain
conditions or with additional assumptions:

 - Entry ordering by insertion, update or access.
 - Hard latency guarantees. e.g. Chronicle Map doesn't specify "all operations complete in less than one
 second." Persistence via memory-mapping implies that any memory write could trigger flushing
 the whole file to disk, that could take long time.
 - Entry expirations. This could be emulated by storing the last update (access) time in the value bytes
 and checking it on each access to Chronicle Map. This could be implemented, for example, by running a background thread that
 traverses the Chronicle Map and cleans expired entries. However it is not guaranteed that Chronicle
 Map would perform better than a cache with "native" expirations.
