# 2. Chronicle Map Data Store Design Overview

## Logic

**Each Chronicle Map is split into N *completely independent*, ordered segments**.
The number of segments is chosen during the Chronicle Map creation and is never changed. Each
segment has an associated 3-level lock (the *read*, *update* and *write* levels). Typical steps of
performing a query to a Chronicle Map:

 1. Compute the hash code of the queried key. Hash code length is 64 bits.
 2. Identify the segment that should hold the key, based on the hash code.
 3. Acquire the segment lock on the needed level.
 4. Search for the entry with the queried key in the segment.
 5. Perform the actual query operation on the entry, if the entry is found (e. g. read the value,
 update the value, etc.), or insert the entry, if the queried key was absent in the segment, and
 insertion of a previously absent entry is implied by the logic of the query being performed.
 6. Release the segment lock.

For multi-key queries,

 1. Identify all the segments in which the involved keys should be held.
 2. Acquire the locks of all the involved segments. Within each involved Chronicle Map store,
 acquire the segment locks in the order of their segments.

 > Acquiring segment locks always in the same order is needed to avoid dead-locks, e. g. when the
 > first thread acquires the lock of the segment #1 and then tries to acquire the lock of the
 > segment #2, and the second thread does the opposite: locks the segment #2, then tries to lock the
 > segment #1.

 3. Perform the query operations.
 4. Release the locks of all the involved segments in the reverse order.

Bulk operations (iterations, background tasks) visit each segment once in any order. For each
segment, they acquire the segment lock, then visit each entry stored in the segment, then release
the segment lock.

### Lock levels

The **read level** is shared, several threads (across OS processes) could hold the segment lock on
the read level at the same time. This level is used for read queries on a single or multiple keys,
like simple value reading by the key. This level couldn't be upgraded to the update or write level.

> The read level couldn't upgrade to the higher levels, because this is dead-lock prone. Two threads
> could acquire the same lock on the read level, then both try to upgrade to the write level - and
> blocked by each other. (Upgrading to the write level through the update level doesn't resolve
> this issue, too.)

The **update level** is partially exclusive: a thread could hold the segment lock on the update
level while some other threads (across OS processes) are holding the same lock on the read level,
but not the update or write level.

This level is used for "typically read" types of queries, which occasionally should be able to write
to the data store, hence need to upgrade to the write level without releasing the lock, for example,
reading value by the key, if the entry is already present in the Chronicle Map, or storing a new
value, if the entry is absent. Also, bulk operations acquire segment locks at least on the update
level, even if they never perform writes.

A segment lock acquired on the update level could be upgraded to the write level.

The **write level** is totally exclusive: when a thread holds the segment lock on the write level,
no other thread (across OS processes) could hold the same lock on any level. The write level is used
for write queries.

## [Lock implementation](3_2-lock-structure.md)

Each lock is represented by two 32-bit words in the Chronicle Map memory. *Count word* holds the
numbers of threads holding the lock at the moment on the read, update and write levels. *Wait word*
holds the number of pending threads, trying to acquire the lock on the write level, at the moment.
These two words are updated only via compare-and-swap operations. In addition to the checks
essential for keeping exclusiveness invariants, described in the previous section, when a thread
tries to acquire the lock on the read level, it checks there are no threads pending for write
access, to prevent starvation of the latter.

As Chronicle Map is designed for inter-process communication, and it doesn't use locks provided by
an operation system, Chronicle Map doesn't implement queues of threads, waiting for lock acquisition
and conditional wake-ups. Threads acquire locks in a spin loop, yielding and/or sleeping after a
certain threshold numbers of unsuccessful attempts.

> Chronicle Map implementations specialized for single-process access to stores may implement
> some kind of thread queues, wake-ups and cooperative scheduling on the runtime level; certain lock
> acquisition strategy is out of scope of the Chronicle Map data store specification.

## The segment data structure

Under normal conditions, a segment is represented by a single *tier*. (*Tier* and *segment tier* are
interchangeable terms in this specification.)

### Segment tier

A segment tier basically consists of two parts:

 1. **Hash lookup**, a flat, power-of-two sized open addressing hash table with collision resolution
 via linear probing, in which the role of keys is played by parts of the Chronicle Map's key hash
 codes (different part of the hash code, from what is used for choosing the segment), values in this
 hash table are allocation identifiers within this segment tier's *entry space*.

 > In the reference Chronicle Map implementation hash lookup's table size is chosen in order to keep
 > table load factor between 0.33 and 0.66.

 2. The **entry space** is the area to allocate blocks of memory to store the actual Chronicle Map's
 entries (key and value bytes sequences), along with some metadata.

 > The specific memory allocation algorithm may vary, depending on the characteristics of the keys
 > and values stored in the specific Chronicle Map, the choice in the context of the time-space
 > tradeoff, etc. One important property that the allocator should obey, is *not* requiring to touch
 > all it's memory up front, for example zeroing out or splitting the memory in blocks, going though
 > all of them and creating an intrusive linked list. Touching entry space memory on demand takes
 > advantage from the Linux memory mapping feature - lazy page allocation. This, in turn, allows to
 > overcommit memory for segment tier's entry spaces without sacrificing actual memory usage much
 > (because hash lookup slots, that should be overcommitted too, are relatively small). This,
 > in turn, mitigates variance of segments' filling rates and makes slow *tier chaining* (see below)
 > very unlikely.

 > The reference Chronicle Map implementation uses the following allocation algorithm: it is
 > initialized with the *chunk size* depending of the configured properties of keys and values,
 > which are going to be stored in the Chronicle Map. The entry space is virtually split into M
 > chunks and a bit set, where each bit corresponds to the single chunk of the same index.
 > The allocation procedure computes the number of chunks that cover the requested allocation size
 > and finds that number of continuous zero bits in the bit set. The allocation identifier is the
 > index of the first chunk of the allocated block.

The hash lookup slots' size is either 4 or 8 bytes. Allocation identifiers use the minimum required
bits to identify themselves within the entry space, the rest bits of hash lookup slots are used to
store bits of the Chronicle Map's key hash codes, to minimize full collisions on the hash lookup
level.

> Hash lookup slots are disallowed to be of any integral byte size from 3 to 8 bytes, despite it
> would be more agile than limiting to 4 or 8 bytes, because cross cache line reads and writes on
> x86 are inherently not atomic, that makes unsafe to perform some write operations with a segment
> locked on the update level. Also, unaligned slots could be corrupted if the writing process is
> interrupted unconditionally, or the operating system fails.

### Tier chaining

If a segment tier is filled up, i. e. on some entry insertion request entry space fails to allocate
a memory block sufficient to place the new entry, *a whole new segment tier is allocated and chained
after the previous tier.* All tiers, either first in their segments or chained, are identical.

> A segment with one or several chained tiers might be much slower for key search, than a "normal"
> segment of a single tier. Tier chaining is an *exceptional* mechanism for dealing with segment
> overflow. Chronicle Map shows it's efficiency properties when a very little fraction or none of
> it's segments has chained tiers. Chronicle Map should be configured accordingly.

### The procedure of searching for a key within a segment

<ol>
 <li>Extract the part of the queried key's hash code, that serves as a key in hash lookup tables of
 the tiers.</li>

 <li>[For each segment tier in the chain, the first tier initially]

  <p>Search for a slot with the extracted hash lookup key in the tier's hash lookup, using
  standard search algorithm for hash tables with open addressing and collision resolution via linear
  probing.

  <ol>
   <li>If such slot is found, go to the entry location within the tier's entry space using the value
   in the found hash lookup slot. Compare the queried key with the key, stored in the entry
   location, byte-by-byte.</li>

   <ol>
    <li>If the keys are identical, the entry for the queried key within the segment is found, exit
    the procedure.</li>
    <li>If the keys are not identical (because of a full collision of parts of their hash codes for
    storing in hash lookup tables), continue the search in the current tier's hash lookup table
    (step #2), without resetting the hash lookup slot index.</li>
   </ol>

   <li>If such slot is not found, and there are no more chained tiers, the entry for the queried key
   within the segment is not found, exit the procedure.

   <p>If there is a chained tier, go to it and start searching again from step #2.</li>
  </ol>
  </li>
</ol>
