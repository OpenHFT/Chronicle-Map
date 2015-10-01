# Chronicle Map Design Overview

## Logic

**Chronicle Map is split into N *completely independent*, ordered segments**. The number of segments
is chosen during Chronicle Map creation and is never changed. Each segment has an associated 3-level
lock (read, *update* and write levels). Typical steps of performing a query to a Chronicle Map:

 1. Compute the hash code of the queried key. Hash code size is 64 bits.
 2. Choose the segment that should hold the key, by the hash code
 3. Acquire the segment lock on the needed level on
 4. Search for the entry with the queried key in the segment
 5. Perform the query operation on the entry (read the value, update the value, etc.)
 6. Release the segment lock

For multi-key queries,

 1. All segments in which the involved keys should be held are identified
 2. Acquire the locks of all involved segments. Within each involved Chronicle Map, segment locks
 are acquired in the order of their segments.

 > Acquiring segment locks in the predictable order is needed to avoid dead-locks, e. g. when the
 > first thread acquires lock of the segment #1 and then tries to acquire the lock of the
 > segment #2, and the second thread does the opposite: locks the segment #2, then tries to lock the
 > segment #1.

 3. Perform the query operations
 4. Release the locks of all involved segment in reverse order

Bulk operations (iterations, background tasks) visit each segment once in any order. For each
segment, they acquire the segment lock, then visit each entry stored in the segment, then release
the segment lock.

### Lock levels

**Read level** is shared, several threads and processes could hold the segment lock on read level at
the same time. This level is used for read queries on a single or multiple keys, like simple value
reading by the key. This level couldn't be upgraded to update or write level.

> Read level couldn't upgrade to more exclusive levels, because this is dead-lock prone. Two threads
> could acquire the same lock on read level, then both try upgrade to write level - and blocked by
> each other. (Upgrading to write level through update level wouldn't eliminate this issue too.)

**Update level** is partially exclusive: a thread could hold the segment lock on update level while
some other threads or processes are holding the same lock on read level, but not update or write
level. This level is used for "typically read" queries, that occasionally should be able to write
hence upgrade to write level without releasing the lock, for example reading value by the key, if
the entry is present or storing a new value, if the entry is absent. Also, bulk operations should
acquire segment locks at least on update level, even if they never perform writes. Lock on update
level could be upgraded to write level.

**Write level** is fully exclusive: when a thread holds the segment lock on write level, no other
thread or process could hold the same lock on any level. Write level is used for write queries.

## Lock implementation

Lock is represented by two 32-bit words in Chronicle Map memory. *Count word* holds the counts of
threads holding the lock at the moment on read, update and write levels. *Wait word* holds the
number of pending threads, trying to acquire the lock on write level, at the moment. These two words
are updated only via compare-and-swap operations. In addition to the checks essential for keeping
exclusiveness invariants, described in the previous section, when a thread tries to acquire the lock
on read level, it should check there are no threads pending for write access, to prevent starvation
of the latter.

As Chronicle Map is designed for inter-process communication and it doesn't use operating system
level locks, it doesn't have queues of threads, waiting for lock acquisition and conditional
wake-ups. Threads should try to acquire a lock in a spin loop, probably yielding and/or sleeping
between the attempts, after certain numbers of unsuccessful attempts.

> Chronicle Map implementations specialized for single-process access may implement thread some kind
> of thread queues and wake-ups on their runtime level; certain lock acquisition strategy is out of
> scope of the Chronicle Map data structure specification.

## The segment data structure

Under normal conditions, a segment is represented by a single *tier*.

### Segment tier

Segment tier basically consists of two parts:

 1. **Hash lookup**, a flat, power-of-two sized open addressing hash table with collision resolution
 via linear probing, in which the role of keys is played by parts of the Chronicle Map's key hash
 codes (different part of the hash code, from what is used for choosing the segment), the values in
 this hash table are allocation identifiers within the segment tier's *entry space*.

 > In the reference Chronicle Map implementation hash lookup table size is chosen in order to keep
 > table load factor between 0.33 and 0.66.

 2. **Entry space** is the area to allocate blocks of memory to store the actual Chronicle Map's
 entries (key and value bytes sequences), along with some metadata.

 > The specific memory allocation algorithm may vary, depending on characteristics of the keys
 > and values stored in the specific Chronicle Map, time-space tradeoff chosen, etc. One important
 > property that the allocator should likely obey, is *not* requiring to touch all it's memory up
 > front, for example zeroing out or splitting the memory in blocks, going though all of them and
 > creating an intrusive linked list. Touching entry space memory on demand takes advantage from
 > the Linux memory mapping feature - lazy page allocation. This, in turn, allows to overcommit
 > memory for segment tier's entry spaces without sacrificing actual memory usage much (because
 > hash lookup slots, that should be overcommited too, are relatively small). This, in turn,
 > mitigates variance of segments' filling rates and makes slow *tier chaining* (see below) very
 > unlikely.

 > The reference Chronicle Map implementation uses the following allocation algorithm: it is
 > initialized with the *chunk size* depending of the configured properties of keys and values,
 > which are going to be stored in the Chronicle Map, virtually splits the entry space into M
 > chunks, and a bit set, where each bit corresponds to a single chunk of the same index. Allocation
 > procedure computes the number of chunks that cover the requested allocation size and finds the
 > same number of continuous zero bits in the bit set. Allocation identifier is the index of the
 > first chunk of the allocated block.

Hash lookup slots' size is either 4 or 8 bytes. Allocation identifiers use the minimum required bits
to identify themselves within the entry space, the rest bits of hash lookup slots are used to store
bits of the Chronicle Map's key hash codes.

### Tier chaining

If segment tier is filled up, i. e. on some entry insertion request entry space fails to allocate
a memory block sufficient to place the new entry, *a whole new segment tier is allocated and chained
after the previous tier.* All tiers, either first in their segments or chained, are identical.

> A segment with one or several chained tiers might be much slower for key search, than a "normal"
> segment with the single tier. Tier chaining is an *exceptional* mechanism for dealing with segment
> overflow. Chronicle Map shows it's efficiency properties when a very little fraction or none of
> it's segments has chained tiers. Chronicle Map should be configured accordingly.

### Procedure of searching for a key within a segment

<ol>
 <li>Extract the part of the queried key's hash code, that should serve as a key in hash lookup
 tables.</li>

  <li>[For a segment tier, the first tier initially]

  <p>Search for the slot with the extracted hash lookup key in the tier's hash lookup, using
  standard search algorithm for hash tables with open addressing and collision resolution via linear
  probing.

  <ol>
   <li>If such slot is found, go to the entry location within the tier's entry space using the value
   in the found hash lookup slot. Compare the queried key with the key, stored in the entry
   location, byte-by-byte.</li>

   <ol>
    <li>If keys are identical, the entry for the queried within the segment is found, exit the
    procedure.</li>

    <li>If keys are not identical (it means parts of their hash codes collide), continue the search
    in the current tier's hash lookup table (step #2), without resetting the slot index.</li>
   </ol>

   <li>If slot is not found, and there are no more chained tiers, the entry for the queried key
   within the segment is not found, exit the procedure.

   <p>If there is a chained tier, go to it and start searching again from step #2.</li>
  </ol>
  </li>
</ol>
