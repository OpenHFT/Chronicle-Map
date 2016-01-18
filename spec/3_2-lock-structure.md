# The Lock Structure

The lock structure is 8 bytes (64 bits) long.

 - Count word
  - Bits 0..29 - read lock count (little-endian)
  - Bit 30 - update lock flag
  - Bit 31 - write lock flag
 - Wait word
  - Bits 32..63 - wait count (little-endian)

## The *try acquire read lock* procedure

 1. Read the lock state.
 2. If the write lock flag is set, or the wait count is non-zero, or the read lock count is
 2<sup>30</sup> &minus; 1, then the procedure fails.
 3. Make a copy of the count word, with the read lock count incremented.
 4. Perform a *compare-and-swap* (CAS) operation on the count word, comparing with the state that
 was read and swapping with the updated, made on the previous step. If the CAS operation succeeds,
 the procedure succeeds. If the operations fails, the procedure fails.

## The *try acquire update lock* procedure

 1. Read the lock state.
 2. If the update or write lock flag is set or the wait count is non-zero, then the procedure fails.
 3. Make a copy of the count word, with the update lock flag set.
 4. Perform a CAS operation on the count word, comparing with the state that was read and swapping
 with the updated, made on the previous step. If the CAS operation succeeds, the procedure succeeds.
 If the operations fails, the procedure fails.

## Time-limited read or update lock acquisition

Attempt to acquire the read or update lock using the corresponding *try acquire* procedure, until it
succeeds, optionally inserting some directives aimed to yield the resources after failed attempts,
for example, x86's `PAUSE` instruction, or an operation system command to schedule another thread to
the current CPU, or a command to clock down the current CPU, etc.

There is no single exact scheme that implementations should follow, it is unspecified. The
implementation may vary for the read and update locks as well.

> The reference Java implementation doesn't insert any resources yielding directives after failed
> try acquire attempts, for both read and update lock acquisition.

Implementations doesn't try to acquire the read or update lock indefinitely, after some finite
number of failed attempts or some finite time elapsed since the time-limited lock acquisition
procedure starts, the procedure fails.

> Indefinite lock acquisition is dead lock prone.

## The *try acquire write lock* procedure

 1. (Optional step) Read the count word of the lock state.
 2. (Optional step) If the count word is non-zero, the procedure fails.
 3. Perform a CAS operation on the count word of the lock state, comparing 0 (i. e. the operation
 fails, if any bit of the count word is non-zero) and swapping with 0x80000000, i. e. a count word
 with the write lock flag set, the update lock flag not set, and the read lock count of zero. The
 result of the CAS operation is the result of the procedure.

The two optional steps should be performed, if the procedure most likely fails, and the steps should
be skipped, if the procedure most likely succeeds. Implementations may have both versions of the
procedure and call one depending on the context.

> When the first two steps of the procedure are performed on x86, the cache line containing the lock
> structure is first moved to the Shared state (in terms of [MESI protocol](
> https://en.wikipedia.org/wiki/MESI_protocol) or it's successors in newer CPUs), then to the
> Modified state. When the first two steps are omitted, the cache line is moved directly to the
> Modified state, reducing bus traffic. On the other hand, when the first two steps are performed
> and cause the try acquire write lock procedure to fail early, bus locking is avoided, that
> increases scalability.

> The reference Java implementation uses only the version of this procedure without the first two
> steps.

## The *try upgrade update to write lock* procedure

Perform a CAS operation on the count word of the lock state, comparing 0x40000000 (i. e. a count
word with the update lock flag set, the write lock flag not set, the read lock count of zero) and
swapping with 0x80000000, i. e. a count word with the write lock flag set, the update lock flag not
set, and the read lock count of zero. The result of the CAS operation is the result of the
procedure.

### The *register wait* procedure

 1. Read the wait word of the lock structure.
 2. If the wait count is 2<sup>31</sup> &minus; 1, the procedure fails: *wait count overflow*.
 3. Perform a CAS operation on the wait word of the lock structure, comparing the wait word that was
 read with a wait word with the wait count incremented. If the CAS operation fails, begin the
 register wait procedure from the start. If the CAS operation succeeds, the procedure succeeds.

### The *deregister wait* procedure

 1. Read the wait word of the lock structure.
 2. If the wait count is 0, the procedure fails: *wait count underflow*.
 3. Perform a CAS operation on the wait word of the lock structure, comparing the wait word that was
 read with a wait word with the wait count decremented. If the CAS operation fails, begin the
 deregister wait procedure from the start. If the CAS operation succeeds, the procedure succeeds.

## Time-limited write lock acquisition or update to write upgrade

 1. Perform the corresponding *try acquire* procedure ([write lock](
 #the-try-acquire-write-lock-procedure) or [upgrade update to write lock](
 #the-try-upgrade-update-to-write-lock-procedure)). If the attempt succeeds, this procedure
 succeeds. If the attempt fails, continue with the following steps.
 2. [Register a wait](#the-register-wait-procedure). If the register wait procedure fails, this
 time-limited procedure fails.
 3. Read the lock state.
 4. If the count word of the lock state is not equal to 0 (in case of acquiring the write lock) or
 0x40000000 (in case of upgrading update to write lock), go to the step 7.
 5. If the wait count is 0, this procedure fails.
 6. Perform a CAS operation on the whole lock structure, comparing the state, read on the step 3,
 with an updated state with the wait count decremented, and count word of 0x80000000 (the write lock
 flag set). If the CAS operation succeeds, the time-limited acquisition succeeds. If the CAS
 operation fails, continue with the step 7.
 7. Check if this time-limited procedure has run out of the time limit or the number of attempts.
 If it is, the time-limited acquisition fails, [deregister a wait](#the-deregister-wait-procedure)
 before exiting from this procedure (the result of the deregister wait procedure doesn't matter).
 Otherwise, continue from the step 3 of this procedure.

Implementations doesn't make locking attempts indefinitely [for the same reasons, as in case of
acquiring read and update lock](#time-limited-read-or-update-lock-acquisition).

> The reference Java implementation:
>
> Attempt operations: [`VanillaReadWriteUpdateWithWaitsLockingStrategy`](
> https://github.com/OpenHFT/Chronicle-Algorithms/blob/chronicle-algorithms-1.1.6/src/main/java/net/openhft/chronicle/algo/locks/VanillaReadWriteUpdateWithWaitsLockingStrategy.java)
>
> Time-limited operations: [`BigSegmentHeader`](
> ../src/main/java/net/openhft/chronicle/hash/impl/BigSegmentHeader.java)
