# The Chronicle Map Key Hashing and Checksum Algorithms

Keys in Chronicle Map are arbitrary sequences of bytes. [xxHash
](https://github.com/Cyan4973/xxHash/) algorithm is applied to the key, to obtain the primary key
hash code. Then the [`hashSplitting`](3_1-header-fields.md#hashsplitting) algorithm is applied, to
determine the segment in which the key should be stored, and the part of the key hash code to be
stored in a segment tier's hash lookup.

> The reference Java implementation: [XxHash_r39
> ](https://github.com/OpenHFT/Chronicle-Algorithms/blob/chronicle-algorithms-1.1.6/src/main/java/net/openhft/chronicle/algo/hashing/XxHash_r39.java).
> Although the Java implementation class has `_r39` suffix, the xxHash algorithm is stable since r3
> and [won't change in the future
> ](https://github.com/Cyan4973/xxHash/issues/34#issuecomment-169176338). A different version of
> the algorithm could have a different name.

## Checksum algorithm

### Primary checksum

The primary checksum is a 64-bit value.

If the 2nd field of the [stored entry structure
](3-memory-layout.md#stored-entry-structure) ends at the same address, as the 6th field of the same
structure starts, i. e. the value size is 0, and the size itself is stored using 0 bytes, and there
is no value alignment, the key hash code *is* the primary checksum.

Otherwise, the [xxHash](https://github.com/Cyan4973/xxHash/) algorithm is applied to the memory
range between the end of the 2nd field of the stored entry structure and the end of the 5th field,
i. e. between the end of the stored key and the end of the stored value. The resulting hash value
is called *payload checksum*.

> xxHash is used to compute the payload checksum instead of CRC32, because the Java implementation
> of CRC32 appears to be slower than xxHash while having the same (if not worse) quality, and the
> native CRC32 implementation, using specialized processor instructions, is available from Java only
> via JNI, having significant per-call costs, that eliminates any benefits.

Then the following procedure is used to compute the primary checksum:
```java
long K2 = 0x9ae16a3b2f90404fL;
long mul = K2 + (keySize << 1);
long a = keyHashCode + K2;
long c = ((payloadChecksum >>> 37) | (payloadChecksum << 27)) * mul + a;
long d = (((a >>> 25) | (a << 39)) + payloadChecksum) * mul;
long a1 = (c ^ d) * mul ^ ((c ^ d) * mul >>> 47);
long primaryChecksum = ((d ^ a1) * mul ^ ((d ^ a1) * mul >>> 47)) * mul;
```
where the `keySize` is the key sequence length in bytes, `keyHashCode` is the key hash code, and
the `payloadChecksum` is the payload checksum, computed on the previous step. The given procedure
code is in [Java language](https://docs.oracle.com/javase/specs/jls/se8/html/index.html).

### Entry checksum

Entry checksum is 4 bytes long, it is obtained by XOR-ing lowest and highest 32 bits of the [primary
checksum[#primary-checksum]. The entry checksum is stored in the 6th field of the [stored entry
structure](3-memory-layout.md#stored-entry-structure).

> The reference Java implementation: [`HashEntryChecksumStrategy`
> ](../src/main/java/net/openhft/chronicle/hash/impl/stage/entry/HashEntryChecksumStrategy.java).