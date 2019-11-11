# 1. Chronicle Map Data Store Design Goals, Assumptions and Guarantees

 - Chronicle Map is an in-memory, highly concurrent key-value store. Generally keys and values are
 arbitrary sequences of bytes.
 - Chronicle Map optionally persists to a *single file* via the memory-mapping facility, present in
 Windows and POSIX-compatible operating systems (`mmap`). The file shouldn't necessarily materialize
 on disk. It's OK for Chronicle Map if the file resides a memory-mounted file system. The whole
 Chronicle Map state is contained in the file contents i. e. it is possible to move, copy or send
 the file to another machine, access it and observe exactly the same Chronicle Map state. Chronicle
 Map doesn't use the metadata of the file.
 - Chronicle Map supports fully-featured concurrent access from multiple processes, mapping the same
 file.
 - Chronicle Map supports *isolated* multi-key operations, either on a single key-value domain
 (a single Chronicle Map store) or several ones. But multi-key updates to persisted Chronicle Map(s)
 are *not* atomic (basically, *isolated* and *atomic* terms used in the sense of [ACID](
 https://en.wikipedia.org/wiki/ACID) here; see the detailed definition below in
 [Guarantees](#guarantees-1) section).
 - Chronicle Map runs only on little-endian architectures.
 - Chronicle Map is *not* distributed: the whole data store resides the memory of a single machine
 (a single file, if persisted).

## Guarantees

### Assumptions

Chronicle Map provides some guarantees under the following assumptions:

#### Threads

Throughout this specification, the term *thread* means the execution thread of the runtime of the
Chronicle Map implementation. It might be an OS thread or a "greener" thread.

 - Operations declared for execution in some threads are performed sequentially in the same order,
 with full memory visibility between operations.

#### CPU

 - CPU supports atomic 64-bit compare-and-swap operations with aligned memory, i. e. if several
 threads, possibly belonging to different processes, try to perform compare-and-swap operation on
 the same 4-byte block, at most one thread will succeed and all the rest will fail.
 - Aligned 32-bit or 64-bit writes are atomic, that means either all 4(8) bytes are written to
 the memory, or nothing is written (in case of power outage, unconditional thread interruption,
 etc.).

The two above points are true for CPUs with x86 and x86_64 architectures.

#### Memory ordering

 - If some values X<sub>i</sub> are written at addresses A<sub>i</sub>, then 32- or 64-bit value Y
 is written at address B, there is a way to ensure some memory order. In particular, if a
 concurrent thread reads value Y from address B, after that it will read X<sub>i</sub values from
 A<sub>i</sub> addresses (but not the old values, stored at those addresses).

 > In terms of the Java Memory model, this assumption means "writes to A<sub>i</sub> addresses
 > *happens-before* the write of Y value at address B, which *synchronizes-with* read of this value
 > from a concurrent thread". I. e. in Java there are some trivial ways to ensure such memory
 > ordering, for example, volatile write or compare-and-swap at address B with volatile read of
 > value Y at address B from a concurrent thread.

#### Memory-mapping

 - The memory-mapping implementation doesn't corrupt mapped files, even if operating system
 execution was interrupted in any way (black out, virtual machine crash, etc.) It means Chronicle
 Map expects some memory might be "stale", i. e. the values written shortly before operating system
 failure might not be persisted, but it doesn't expect to read values that have never been written
 to the memory.
 - Writes to the disk are at least 4 or 8 bytes atomic, i. e. either aligned 4- or 8-byte blocks
 are fully written to the disk, or not written at all (in case of power loss, virtual machine crash,
 etc.).

 > Modern non-volatile drives (HDDs and SSDs) support atomic sector writes, which are usually at
 > least 512 bytes in size.

Chronicle Map *doesn't* assume anything specific about how memory if flushed to the disk:

 - The order in which memory is flushed (up or down by addresses, random, concurrent, etc.).
 - The maximum timeout dirty memory might not be flushed to the disk, i. e. Chronicle Map assumes
 some memory might remain dirty forever.
 - The maximum fraction of dirty memory not written to the disk, i. e. Chronicle Map assumes that
 *all* the mapped memory might be dirty.

#### File locking

The operating system supports *advisory* exclusive [file locking](
https://en.wikipedia.org/wiki/File_locking).

### Guarantees

If the above assumptions are met, Chronicle Map aims to satisfy the following guarantees:

 - Single-key and multi-key accesses and updates to the Chronicle Map (multi-key updates could span
 several Chronicle Map stores) are concurrently isolated, i. e. accesses involving a certain key
 are totally ordered across accessing threads and processes. All updates made to the entry during
 the previous update are visible during the subsequent accesses. After a multi-key update,
 a subsequent multi-key query involving a subset of the updated keys (and possibly some more keys)
 observes all updates made to all the entries corresponding to the keys from that subset.
 - Under any conditions (concurrent access, inter-process access) and using any type of access
 (querying the entry by the key or iteration), only entries that were ever stored in the Chronicle
 Map store are observed. Reading corrupted or half written values by some keys, or observing some
 keys that were never stored during the iteration is impossible.

If the Chronicle Map store is persisted to the file and the operating system fails to flush all
the dirty memory to the disk due to a power-off or any other failure, when the file is mapped to
the memory and accessed again, it is required to perform a *special recovery procedure* on the
Chronicle Map first, which identifies and purges corrupted entries from the Chronicle Map.
Therefore, *some entries updated shortly before the failure could be lost.*

 > The aforementioned *special recovery procedure* is `ChronicleHashBuilder.recoverPersistedTo()`
 > in the reference Java implementation. See [Recovery](../docs/CM_Tutorial.adoc#recovery) section in the
 > Chronicle Map tutorial for more information.

## Goals

The ultimate goal of Chronicle Map design is efficiency:

 - If the number of entries in Chronicle Map is much greater than the number of accessor CPUs, it
 scales (i. e. adding the last CPU still adds to the total throughput) up to the total number of
 CPUs present in the system.
 - Chronicle Map is a low-latency key-value store, meaning that for any particular percentile (90%,
 99%, 99.9% etc.) the latency of Chronicle Map operations should be the best or among the best
 compared to latencies of other key-value stores filled with the same data and providing the same or
 stronger guarantees.
 - Chronicle Map memory and disk space overuse rate is limited by 25% for any range and distribution
 of key and values lengths (if configured properly). Chronicle Map exhibits the least memory
 footprint among key-value stores, if keys and values have constant lengths.

### Assumptions

Chronicle Map targets the goals outlined above under the following assumptions:

 - It runs on AMD64 (aka x86_64) architecture
 - It runs on Linux
 - The total volume of memory, used by the Chronicle Map fits the available physical memory.
 In other words, the memory accessed by the Chronicle Map is never evicted to the disk.

If these assumptions are not held, Chronicle Map will still operate correctly and provide the same
guarantees, but it shouldn't be expected to be similarly efficient.

## Non-goals

 - Alphabetical ordering of the keys (sorted keys).
 - Durability. There are ways to make Chronicle Map look like a durable key-value store, e. g.
 to call `msync` after each operation. However, if required, it would be better to use a data store
 designed for durability.

The following items are currently non-goals, but might become goals in the future, at least under
certain conditions or assumptions:

 - Entry ordering by insertion, update or access times.
 - Hard latency guarantees. Currently Chronicle Map doesn't specify "all operations complete in less
 than one second." Persistence via memory-mapping implies that any memory write could trigger
 flushing the whole file to the disk, that could take a long time.
 - Entry expirations. Currently entry expirations could be emulated by storing the last update
 (access) time in the value bytes, checking it on each access to the Chronicle Map and, for example,
 running a background thread that traverses the Chronicle Map and cleans expired entries.
