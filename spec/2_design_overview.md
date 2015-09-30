# Chronicle Map Design Overview

## Logic

Chronicle Map is split into N *completely independent*, ordered segments. The number of segments is
chosen during Chronicle Map creation and is never changed. Each segment has an associated 3-level
lock (read, *update* and write levels). Typical steps of performing a query to a Chronicle Map:

 1. Compute the hash code of the queried key
 2. Choose the segment that should hold the key, by the hash code
 3. Acquire the segment lock on the needed level on
 4. Search for the entry with the queried key in the segment
 5. Perform the query operation on the entry (read the value, update the value, etc.)
 6. Release the segment lock

For multi-key queries,

 1. All segments in which the involved keys should be held are identified
 2. Acquire the locks of all involved segments. Within each involved Chronicle Map segment locks
 are acquired in the order of their segments.

 > Acquiring segment locks in the predictable order is needed to avoid dead-locks, e. g. when first
 > thread acquires lock of the segment #1 and then tries to acquire the lock of the segment #2, and
 > the second thread does the opposite: locks the segment #2, then tries to lock the segment #1.

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

Lock is represented by two 32-bit words in Chronicle Map memory. *Count word* holds the the counts
of threads holding the lock at the moment on read, update and write levels. *Wait word* holds the
number of pending threads, trying to acquire the lock on write level, at the moment. These two words
are updated only via compare-and-swap operations. In addition to the checks essential for keeping
exclusiveness invariants, described in the previous section, when a thread tries to acquire the lock
on read level, it should check there are no threads pending for write access, to prevent starvation
of the latter.

## The segment data structure

Under normal conditions, a segment is represented by a single *tier*.

Segment tier basically consists of two parts: *hash lookup*, a flat, power-of-two sized open
addressing hash table

