# Chronicle Map

**Documentation: <a href="#chronicle-map-3-tutorial">Tutorial</a>,
<a href="http://www.javadoc.io/doc/net.openhft/chronicle-map/">Javadoc</a>**

**Community support: <a href="https://github.com/OpenHFT/Chronicle-Map/issues">Issues</a>,
<a href="https://groups.google.com/forum/#!forum/chronicle-map">Chronicle Map mailing list</a>, <a href="http://stackoverflow.com/tags/chronicle-map">Stackoverflow</a>,
<a href="https://plus.google.com/communities/111431452027706917722">Chronicle User's group</a>**

<img align="right" src="http://openhft.net/wp-content/uploads/2014/07/ChronicleMap_200px.png">

[Chronicle Map usage heatmap](http://jrvis.com/red-dwarf/?user=openhft&repo=Chronicle-Map)

### 3 min to understand everything about Chronicle Map

Chronicle Map is an in-memory key-value store designed for low-latency and/or multi-process
applications. Notably trading, financial market applications.

**Features**
 - **Ultra low latency**: Chronicle Map targets median latency of both read and write queries of less
 than 1 *micro*second in [certain tests](
 https://github.com/OpenHFT/Chronicle-Map/search?l=java&q=perf&type=Code).
 - **High concurrency**: write queries scale well up to the number of hardware execution threads in
 the server. Read queries never block each other.
 - (Optional) **persistence to disk**
 - **Multi-key queries**
 - (Optional, closed-source) eventually-consistent, fully-redundant, asynchronous replication across
 servers, "last write wins" strategy by default, allows to implement custom [state-based CRDT](
 https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type) strategy.

**Unique features**
 - Multiple processes could access a Chronicle Map concurrently. At the same time,
 the data store is *in-process for each of the accessing processes*. (Out-of-process approach to IPC
 is simply incompatible with Chronicle Map's median latency target of < 1 μs.)

 - Replication *without logs*, with constant footprint cost, guarantees progress even if the network
 doesn't sustain write rates.

<b><i>Chronicle Map</i> has two meanings:</b> [the language-agnostic data store](spec) and [the
implementation of this data store for the JVM](src). Currently, this is the only implementation.

**From Java perspective,** `ChronicleMap` is a `ConcurrentMap` implementation which stores the
entries *off-heap*, serializing/deserializing key and value objects to/from off-heap memory
transparently. Chronicle Map supports
 - Key and value objects caching/reusing for making *zero allocations (garbage) on
queries*.
 - Flyweight values for eliminating serialization/deserialization cost and allowing direct
 read/write access to off-heap memory.

**Primary Chronicle Map use cases**
 - Replacing slower key-value stores, like Redis and Memcached, when used within a single server.
 - Replacing similar JVM-centric solutions, like Coherence and Hazelcast, for speed and/or certain
 Chronicle Map features those solutions lack.
 - *Moving parts of the application state out of the Java heap* for either
   - Reducing the heap size, for reducing GC pressure, or fitting 32 GB for using Compressed Oops
   - Inter-process communication
   - Persistence
   - Replication across servers
 - Drop-in `ConcurrentHashMap` replacement, Chronicle Map performs better in some cases.

**What guarantees does Chronicle Map provide in ACID terms?**

 - Atomicity - single-key queries are atomic if Chronicle Map is properly configured, multi-key
 queries are not atomic.
 - Consistency - doesn't make sense for key-value stores
 - Isolation - yes (for both single- and multi-key queries).
 - Durability - no, Chronicle Map can be persisted to disk but with no guarantee as to to how frequently this 
 happens - this is under the control of the OS. All data is guaranteed to be written to disk when the Map is closed.
 **Clustering and replication for Chronicle
 Map is provided by [Chronicle Map Enterprise](http://chronicle.software/products/chronicle-map/).** 
 
**Project status: ready for production.**

**What is the Chronicle Map's data structure?** In one sentence and simplified, a Chronicle Map
data store is a big chunk of shared memory (optionally mapped to disk), split into independent
segments, each segment has an independent memory allocation for storing the entries, a hash table
for search, and a lock in shared memory (implemented via CAS loops) for managing concurrent access.
Read [the Chronicle Map data store design overview](spec/2-design-overview.md) for more.

### Chronicle Map is *not*

 - A document store. No secondary indexes.
 - A [multimap](https://en.wikipedia.org/wiki/Multimap). Using a `ChronicleMap<K, Collection<V>>`
 as multimap is technically possible, but often leads to problems (see [a StackOverflow answer](
 http://stackoverflow.com/a/36486525/648955) for details). Developing a proper multimap with
 Chronicle Map's design principles is possible, [contact us](mailto:sales@chronicle.software) if
 you would consider sponsoring such development.

Chronicle Map doesn't support

 - Range queries, iteration over the entries in alphabetical order. Keys in Chronicle Map are not
 sorted.
 - LRU entry eviction

### Features
<table>
  <tr>
    <th align="left">Feature</th>
    <th>Availability</th>
  </tr>
  <tr>
    <td>In-memory off-heap Map</td>
    <th rowspan="2">Open-source<brChronicle Map</th>
  </tr>
  <tr>
    <td>Persistence to disk</td>
  </tr>
  <tr>
    <td>Remote calls</td>
    <th rowspan="3"><a href="http://chronicle.software/products/chronicle-map/">
    Closed-source</a></th>
  </tr>
  <tr>
    <td>Eventually-consistent replication (100% redundancy)</td>
  </tr>
  <tr>
    <td>Synchronous replication</td>
  </tr>
  <tr>
    <td>Partially-redundant replication</td>
    <th rowspan="2"><a href="http://chronicle.software/consultancy/"><br>On-demand</a></th>
  </tr>
  <tr>
    <td>Entry expiration timeouts</td>
  </tr>
</table>

### License

Chronicle Map is distributed under LGPLv3. If you want to obtain this software under more permissive
license, please contact sales@chronicle.software.

### Peer projects
 - [Chronicle Engine](https://github.com/OpenHFT/Chronicle-Engine) - reactive processing framework
 supporting Chronicle Map as a backend.
 - [Chronicle Enterprise](http://chronicle.software/products/chronicle-enterprise/) - extended
 version of Chronicle Engine.
 - [Chronicle Journal](
 http://vanillajava.blogspot.com/2015/09/chronicle-journal-customizable-data.html) - another
 key-value built by Chronicle Software, with different properties.

## Chronicle Map 3 Tutorial

#### Contents
 - [Difference between Chronicle Map 2 and 3](#difference-between-chronicle-map-2-and-3)
 - [Download the library](#download-the-library)
 - [Create a `ChronicleMap` Instance](#create-a-chroniclemap-instance)
   - [In-memory Chronicle Map](#in-memory-chronicle-map)
   - [Persisted Chronicle Map](#persisted-chronicle-map)
   - [Recovery](#recovery)
   - [Key and Value Types](#key-and-value-types)
     - [Custom serializers](#custom-serializers)
        - [Custom `CharSequence` encoding](#custom-charsequence-encoding)
        - [Custom serialization checklist](#custom-serialization-checklist)
 - [`ChronicleMap` usage patterns](#chroniclemap-usage-patterns)
   - [Single-key queries](#single-key-queries)
   - [Multi-key queries](#multi-key-queries)
   - [Entry checksums](#entry-checksums)
 - [Close `ChronicleMap`](#close-chroniclemap)
 - [Behaviour Customization](#behaviour-customization)
   - [Example - Simple logging](#example---simple-logging)
   - [Example - BiMap](#example---bimap)
   - [Example - Monitor Chronicle Map statistics](#example---monitor-chronicle-map-statistics)

### Difference between Chronicle Map 2 and 3

Functional changes in Chronicle Map 3:

 - Chronicle Map 3 has [formal data store specification](spec), that verbalizes [the guarantees
 which the data store provides](spec/1-design-goals.md#guarantees-1) and gives a way to verify those
 guarantees.
 - Added support for multi-key queries.
 - "Listeners" mechanism fully reworked, see the [Behaviour Customization](#behaviour-customization)
 section. This has a number of important consequences, most notable is:
   - Possibility to define replication eventual-consistency strategy, different from "last write
   wins", e. g. any state-based CRDT.
 - "Stateless clients" functionality (i. e. remote calls) is moved to [Chronicle Engine](
 https://github.com/OpenHFT/Chronicle-Engine).
 - Replication is done via [Chronicle Engine](https://github.com/OpenHFT/Chronicle-Engine).
 - Chronicle Map 2 has hard creation-time limit on the number of entries storable in a Chronicle
 Map. If the size exceeds this limit, an exception is thrown. In Chronicle Map 3, this limitation
 is removed, though the number of entries still has to be configured on the Chronicle Map creation,
 exceeding this configured limit is possible, but discouraged. See the [Number of entries
 configuration](#number-of-entries-configuration) section.
 - Chronicle Map 3 supports [entry checksums](#entry-checksums) that allows to detect data
 corruption, hence brings additional safety.
 - Chronicle Map 3 allows to [recover](#recovery) after failures and corruptions.
 - A number of smaller improvements and fixes.

Non-functional changes:

 - Chronicle Map 3 requires Java version 8 or newer, while Chronicle Map 2 supports Java 7.
 - Chronicle Map 3 has [versioning policy](docs/versioning.md) and [compatibility policy](
 docs/compatibility.md). Chronicle Map 2 doesn't have such documents.

If you use Chronicle Map 2, you might be looking for [Chronicle Map 2 Tutorial](
https://github.com/OpenHFT/Chronicle-Map/tree/2.1#contents) or [Chronicle Map 2 Javadoc](
http://www.javadoc.io/doc/net.openhft/chronicle-map/2.3.9/).

### Download the library

#### Maven Artifact Download
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/net.openhft/chronicle-map/badge.svg)](https://maven-badges.herokuapp.com/maven-central/net.openhft/chronicle-map)
```xml
<dependency>
  <groupId>net.openhft</groupId>
  <artifactId>chronicle-map</artifactId>
  <version><!--replace with the latest version--></version>
</dependency>
```
Click here to get the [Latest Version Number](
http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22net.openhft%22%20AND%20a%3A%22chronicle-map%22)

#### Maven Snapshot Download
If you want to try out the latest pre-release code, you can download the snapshot artifact manually
from https://oss.sonatype.org/content/repositories/snapshots/net/openhft/chronicle-map/.
A better way is to add the following to your `setting.xml`, to allow maven to download snapshots:

```xml
<repository>
    <id>Snapshot Repository</id>
    <name>Snapshot Repository</name>
    <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    <snapshots>
        <enabled>true</enabled>
    </snapshots>
</repository>
```
and define the snapshot version in your pom.xml, for example:
```xml
<dependency>
  <groupId>net.openhft</groupId>
  <artifactId>chronicle-map</artifactId>
  <version><!--replace with the latest snapshot version--></version>
</dependency>
```

### Create a `ChronicleMap` Instance

Creating an instance of `ChronicleMap` is a little more complex than just calling a constructor.
To create an instance you have to use the `ChronicleMapBuilder`.

#### In-memory Chronicle Map

``` java
import net.openhft.chronicle.map.*
.....

interface PostalCodeRange {
    int minCode();
    void minCode(int minCode);

    int maxCode();
    void maxCode(int maxCode);
}

ChronicleMapBuilder<CharSequence, PostalCodeRange> cityPostalCodesMapBuilder =
    ChronicleMapBuilder.of(CharSequence.class, PostalCodeRange.class)
        .name("city-postal-codes-map")
        .averageKey("Amsterdam")
        .entries(50_000);
ChronicleMap<CharSequence, PostalCodeRange> cityPostalCodes =
    cityPostalCodesMapBuilder.create();

// Or shorter form, without builder variable extraction:

ChronicleMap<Integer, PostalCodeRange> cityPostalCodes = ChronicleMap
    .of(CharSequence.class, PostalCodeRange.class)
    .name("city-postal-codes-map")
    .averageKey("Amsterdam")
    .entries(50_000)
    .create();
```

This snippet creates an in-memory Chronicle Map store, supposed to store about 50 000 *city name ->
postal code* mappings. It is accessible within a single JVM process - the process it is created
within. The data is accessible while the process is alive, when the process is terminated, the data
is vanished.

#### Persisted Chronicle Map

Replace `.create()` calls with `.createPersistedTo(cityPostalCodesFile)`, if you want the Chronicle
Map to either

 - Outlive the process it was created within, e. g. to support hot application redeploy
 - Be accessible from *multiple concurrent processes* on the same server
 - Persist the data to disk

The `cityPostalCodesFile` has to represent the same location on your server among all Java
processes, wishing to access this Chronicle Map instance, e. g.
`System.getProperty("java.io.tmpdir") + "/cityPostalCodes.dat"`.

The name and location of the file is entirely up to you.

Note than when you create a `ChronicleMap` instance with `.createPersistedTo(file)`, and the given
file already exists in the system, you *open a view to the existing Chronicle Map data store from
this JVM process* rather than creating a new Chronicle Map data store. I. e. it could already
contain some entries. No special action with the data is performed during such operation. If you
want to clean up corrupted entries and ensure that the data store is in correct state, see
[Recovery](#recovery) section.

 > ##### "`ChronicleMap` instance" vs "Chronicle Map data store"
 >
 > In this tutorial, *`ChronicleMap` instance* (or simply *`ChronicleMap`*) term is used to refer to
 > *on-heap object*, providing access to a *Chronicle Map data store* (or *Chronicle Map key-value
 > store*, or *Chronicle Map store*, or simply *Chronicle Map*, with space between two words in
 > contrast to `ChronicleMap`), which could be purely in-memory, or persisted to disk. Currently
 > Java implementation doesn't allow to create multiple accessor `ChronicleMap` objects to a single
 > *in-memory Chronicle Map store*, i. e. there is always a one-to-one correspondence, that may lead
 > to confusion. *Persisted Chronicle Map store*, however, allows to create multiple accessor
 > *`ChronicleMap` instances* either within a single JVM process (though [it is not recommended to
 > do that](#single-chroniclemap-instance-per-jvm)), or from concurrent JVM processes, that is
 > perfectly OK.

When no processes access the file, it could be freely moved to another location in the system, and
even to another server, even running different operating system, opened from another location and
you will observe the same data.

If you don't need the Chronicle Map instance to survive the server restart, i. e. you don't need
persistence to disk, only multi-process access, choose the file to be mounted on [tmpfs](
http://en.wikipedia.org/wiki/Tmpfs), e. g. on Linux it is as easy as placing you file in `/dev/shm`
directory.

---

<a name="number-of-entries-configuration"></a>
**You *must* configure `.entries(entries)` -- the supposed `ChronicleMap` size.** Try to configure
the `entries` so that the created Chronicle Map is going to serve about 99% requests being less or
equal than this number of entries in size.

*You shouldn't put additional margin over the actual target number of entries.* This bad practice
was popularized by `new HashMap(capacity)` and `new HashSet(capacity)` constructors, which accept
capacity, that should be multiplied by load factor to obtain the actual maximum expected number of
entries in the container. `ChronicleMap` and `ChronicleSet` don't have a notion of load factor.

See `ChronicleMapBuilder#entries()` [Javadocs](http://www.javadoc.io/doc/net.openhft/chronicle-map/)
for more.

---

**Once `ChronicleMap` instance is created, it's configurations are sealed and couldn't be changed
though the `ChronicleMapBuilder` instance.**

---

<a name="single-chroniclemap-instance-per-jvm"></a>
**Single `ChronicleMap` instance per JVM.** If you want to access a Chronicle Map data store
concurrently within a Java process, you should *not* create a separate `ChronicleMap` instance per
thread. Within the JVM environment, `ChronicleMap` instance *is* a `ConcurrentMap`, and could be
accessed concurrently the same way as e. g. `ConcurrentHashMap`.

#### Recovery

If a process, accessing a persisted Chronicle Map, terminated abnormally: crashed, `SIGKILL`ed, or
terminated because the host operating system crashed, or the machine lost power, the Chronicle Map
might remain in an inaccessible or corrupted state. When the Chronicle Map is opened next time from
another process, it should be done via `.recoverPersistedTo()` method in `ChronicleMapBuilder`.
Unlike `createPersistedTo()`, this method scans all memory of Chronicle Map store for
inconsistencies, if some found, it cleans them up.

*`.recoverPersistedTo()` needs to access the Chronicle Map exclusively. If a concurrent process is
accessing the Chronicle Map while another process is attempting to perform recovery, result of
operations on the accessing process side, and results of recovery are unspecified. The data could be
corrupted further. You* must *ensure no other process is accessing the Chronicle Map store when
calling for `.recoverPersistedTo()` on this store.*

Example:

```java
ChronicleMap<Integer, PostalCodeRange> cityPostalCodes = ChronicleMap
    .of(CharSequence.class, PostalCodeRange.class)
    .name("city-postal-codes-map")
    .averageKey("Amsterdam")
    .entries(50_000)
    .recoverPersistedTo(cityPostalCodesFile, false);
```

The second parameter in `recoverPersistedTo()` method is called
`sameBuilderConfigAndLibraryVersion`, it could be `true` only if `ChronicleMapBuilder` is configured
in exactly the same way, as when the Chronicle Map (persisted to the given file) was created, **and
using the same version of the Chronicle Map library**, or `false`, if initial configurations are not
known, *or current version of Chronicle Map library could be different from the version, used to
create this Chronicle Map initially.*

If `sameBuilderConfigAndLibraryVersion` is `true`, `recoverPersistedTo()` "knows" all the right
configurations and what should be written to the header. It checks if the recovered Chronicle Map's
header memory (containing serialized configurations) is corrupted or not. If the header is
corrupted, it is overridden, and the recovery process continues.

If `sameBuilderConfigAndLibraryVersion` is `false`, `recoverPersistedTo()` relies on the
configurations written to the Chronicle Map's header, assuming it is not corrupted. If it is
corrupted, `ChronicleHashRecoveryFailedException` is thrown.

However, the subject header memory is never updated on ordinary operations with Chronicle Map, so it
couldn't be corrupted if an accessing process crashed, or the operating system crashed, or even the
machine lost power. Only hardware memory or disk corruption or a bug in the file system could lead
to Chronicle Map header memory corruption.

`.recoverPersistedTo()` is harmless if the previous process accessing the Chronicle Map terminated
normally, however *this is a computationally expensive procedure that should generally be avoided.*

Chronicle Map creation and recovery could be conveniently merged using a single call:
`.createOrRecoverPersistedTo(persistenceFile, sameLibraryVersion)` in `ChronicleMapBuilder`, which
acts like `createPersistedTo(persistenceFile)`, if the persistence file doesn't yet exist, and like
`recoverPersistedTo(persistenceFile, sameLibraryVersion)`, if the file already exists, e. g.:

```java
ChronicleMap<Integer, PostalCodeRange> cityPostalCodes = ChronicleMap
    .of(CharSequence.class, PostalCodeRange.class)
    .averageKey("Amsterdam")
    .entries(50_000)
    .createOrRecoverPersistedTo(cityPostalCodesFile, false);
```

If the Chronicle Map is configured to store entry checksums along with entries, recovery procedure
checks for each entry that the checksums is correct, otherwise it assumes the entry is corrupted and
deletes it from the Chronicle Map. If checksums are to stored, recovery procedure cannot guarantee
correctness of entry data. See [Entry checksums](#entry-checksums) section for more information.

#### Key and Value Types

Either key or value type of `ChronicleMap<K, V>` could be:

 - Types with best possible out of the box support:
   - Any [value interface](https://github.com/OpenHFT/Chronicle-Values)
   - Any class implementing [`Byteable`](
   http://openhft.github.io/Chronicle-Bytes/apidocs/net/openhft/chronicle/bytes/Byteable.html)
   interface from [Chronicle Bytes](https://github.com/OpenHFT/Chronicle-Bytes)
   - Any class implementing [`BytesMarshallable`](
   http://openhft.github.io/Chronicle-Bytes/apidocs/net/openhft/chronicle/bytes/BytesMarshallable.html)
   interface from Chronicle Bytes. The implementation class should have a public no-arg constructor.
   - `byte[]` and `ByteBuffer`
   - `CharSequence`, `String` and `StringBuilder`. Note that these char sequence types are
   serialized using UTF-8 encoding by default. If you need a different encoding, refer to the
   example in the [custom `CharSequence` encoding](#custom-charsequence-encoding) section.
   - `Integer`, `Long` and `Double`

 - Types supported out of the box, but not particularly efficiently. You might want to implement
 more efficient [custom serializers](#custom-serializers) for them:
    - Any class implementing `java.io.Externalizable`. The implementation class should have a public
    no-arg constructor.
    - Any type implementing `java.io.Serializable`, including boxed primitive types (except listed
    above) and array types

 - Any other type, if [custom serializers](#custom-serializers) are provided.

**Prefer [value interfaces](https://github.com/OpenHFT/Chronicle-Values).** They don't generate
garbage and have close to zero serialization/deserialization costs. Prefer them even to boxed
primitives, for example, try to use `net.openhft.chronicle.core.values.IntValue` instead of
`Integer`.

**Generally, you *must* hint the `ChronicleMapBuilder` with the average sizes of the keys and
values,** which are going to be inserted into the `ChronicleMap`. This is needed to allocate the
proper volume of the shared memory. Do this via `averageKey()` (preferred) or `averageKeySize()` and
`averageValue()` or `averageValueSize()` respectively.

See the example above: `averageKey("Amsterdam")` is called, because it is assumed that "Amsterdam"
(9 bytes in UTF-8 encoding) is the average length for city names, some names are shorter (Tokyo,
5 bytes), some names are longer (San Francisco, 13 bytes).

Another example: if values in your `ChronicleMap` are adjacency lists of some social graph, where
nodes are represented as `long` ids, and adjacency lists are `long[]` arrays. The average number of
friends is 150. Configure the `ChronicleMap` as follows:
```java
Map<Long, long[]> socialGraph = ChronicleMap
    .of(Long.class, long[].class)
    .name("social-graph-map")
    .entries(1_000_000_000L)
    .averageValue(new long[150])
    .create();
```

**You could omit specifying key or value average sizes, if their types are boxed Java primitives or
value interfaces.** They are constantly-sized and Chronicle Map knows about that.

If the key or value type is constantly sized, or keys or values only of a certain size appear in
your Chronicle Map domain, you should prefer to configure `constantKeySizeBySample()` or
`constantValueSizeBySample()`, instead of `averageKey()` or `averageValue()`, for example:
```java
ChronicleSet<UUID> uuids =
    ChronicleSet.of(UUID.class)
        .name("uuids")
        // All UUIDs take 16 bytes.
        .constantKeySizeBySample(UUID.randomUUID())
        .entries(1_000_000)
        .create();
```

#### Custom serializers

Chronicle Map allows to configure custom marshallers for key or value types which are not supported
out of the box, or serialize supported types like `String` in some custom way (i. e. in encoding,
different from UTF-8), or serialize supported types more efficiently than it is done by default.

There are three pairs of serialization interfaces, only one of them should be implemented and
provided to the `ChronicleMapBuilder` for the key or value type:

##### `BytesWriter` and `BytesReader`

This pair of interfaces is configured via `ChronicleMapBuilder.keyMarshallers()` or
`valueMarshallers()` for the key or value type of the map, respectively.

This pair of interfaces is the most suitable, if the size of the serialized form is not known
in advance, i. e. the easiest way to compute the size of the serialized form of some object of the
type, is performing serialization itself and looking at the number of written bytes.

This pair of interfaces is the least efficient and the simplest to implement, so it should also be
used when efficiency is not the top priority, or when gains of using other pairs of interfaces
(which are more complicated to implement) are marginal.

Basically you should implement two serialization methods:

 - `void write(Bytes out, @NotNull T toWrite)` from `BytesWriter` interface, which writes the given
 `toWrite` instance of the serialized type to the given `out` bytes sink.
 - `T read(Bytes in, @Nullable T using)` from `BytesReader` interface, which reads the serialized
 object into the given `using` instance (if the serialized type is reusable, the `using` object is
 not `null`, and suitable for reusing for this particular serialized object), or a newly created
 instance. The returned object contains the serialized data; it may be identical or not identical to
 the passed `using` instance.

For example, here is the implementation of `BytesWriter` and `BytesReader` for `CharSequence[]`
value type (array of CharSequences):

```java
public final class CharSequenceArrayBytesMarshaller
        implements BytesWriter<CharSequence[]>, BytesReader<CharSequence[]>,
        ReadResolvable<CharSequenceArrayBytesMarshaller> {

    static final CharSequenceArrayBytesMarshaller INSTANCE = new CharSequenceArrayBytesMarshaller();

    private CharSequenceArrayBytesMarshaller() {}

    @Override
    public void write(Bytes out, @NotNull CharSequence[] toWrite) {
        out.writeInt(toWrite.length);
        for (CharSequence cs : toWrite) {
            // Assume elements non-null for simplicity
            Objects.requireNonNull(cs);
            out.writeUtf8(cs);
        }
    }

    @NotNull
    @Override
    public CharSequence[] read(Bytes in, @Nullable CharSequence[] using) {
        int len = in.readInt();
        if (using == null)
            using = new CharSequence[len];
        if (using.length != len)
            using = Arrays.copyOf(using, len);
        for (int i = 0; i < len; i++) {
            CharSequence cs = using[i];
            if (cs instanceof StringBuilder) {
                in.readUtf8((StringBuilder) cs);
            } else {
                StringBuilder sb = new StringBuilder(0);
                in.readUtf8(sb);
                using[i] = sb;
            }
        }
        return using;
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        // no fields to write
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        // no fields to read
    }

    @Override
    public CharSequenceArrayBytesMarshaller readResolve() {
        return INSTANCE;
    }
}
```

Usage example:

```java
try (ChronicleMap<String, CharSequence[]> map = ChronicleMap
        .of(String.class, CharSequence[].class)
        .averageKey("fruits")
        .valueMarshaller(CharSequenceArrayBytesMarshaller.INSTANCE)
        .averageValue(new CharSequence[]{"banana", "pineapple"})
        .entries(2)
        .create()) {
    map.put("fruits", new CharSequence[]{"banana", "pineapple"});
    map.put("vegetables", new CharSequence[] {"carrot", "potato"});
    Assert.assertEquals(2, map.get("fruits").length);
    Assert.assertEquals(2, map.get("vegetables").length);
}
```

The total size of serialization form for some `CharSequence[]` array is 4 bytes for storing the
array length, plus the sum of sizes of all CharSequences, in UTF-8 encoding. Computing this size
without actual encoding has comparable computational cost with performing actual encoding, that
makes `CharSequence[]` type to meet the second criteria (see above) which makes `BytesWriter` and
`BytesReader` the most suitable pair of serialization interfaces to implement for the type.

Note how `read()` implementation attempts to reuse not only the array object, but also the elements,
minimizing the amount of produced garbage. This is a recommended practice.

Some additional notes:

 - If the reader or writer interface implementation is not configurable and doesn't have
 per-instance cache or state fields, i. e. it doesn't have instance fields at all, there is
 a convention to make such implementation classes `final`, give them a `private` constructor and
 expose a single `INSTANCE` constant - a sole instance of this implementation in the JVM.
   - *Don't* make marshaller class `enum`, because there are some issues with `enum` serialization/
   deserialization.
   - For such no-state implementations, don't forget to implement `ReadResolvable`
   interface and return `INSTANCE`, otherwise you have no guarantee that `INSTANCE` constant is the
   only alive instance of this implementation in the JVM.
 - If *both* writer and reader interface implementations have no fields, it might be a good idea
 to merge them into a single type, in order to keep writing and reading logic together.

###### Custom `CharSequence` encoding

Another example shows how to serialize `CharSequence`s using custom encoding (rather than UTF-8):

Writer:

```java
public final class CharSequenceCustomEncodingBytesWriter
        implements BytesWriter<CharSequence>,
        StatefulCopyable<CharSequenceCustomEncodingBytesWriter> {

    // config fields, non-final because read in readMarshallable()
    private Charset charset;
    private int inputBufferSize;

    // cache fields
    private transient CharsetEncoder charsetEncoder;
    private transient CharBuffer inputBuffer;
    private transient ByteBuffer outputBuffer;

    public CharSequenceCustomEncodingBytesWriter(Charset charset, int inputBufferSize) {
        this.charset = charset;
        this.inputBufferSize = inputBufferSize;
        initTransients();
    }

    private void initTransients() {
        charsetEncoder = charset.newEncoder();
        inputBuffer = CharBuffer.allocate(inputBufferSize);
        int outputBufferSize = (int) (inputBufferSize * charsetEncoder.averageBytesPerChar());
        outputBuffer = ByteBuffer.allocate(outputBufferSize);
    }

    @Override
    public void write(Bytes out, @NotNull CharSequence cs) {
        // Write the actual cs length for accurate StringBuilder.ensureCapacity() while reading
        out.writeStopBit(cs.length());
        long encodedSizePos = out.writePosition();
        out.writeSkip(4);
        charsetEncoder.reset();
        inputBuffer.clear();
        outputBuffer.clear();
        int csPos = 0;
        boolean endOfInput = false;
        // this loop inspired by the CharsetEncoder.encode(CharBuffer) implementation
        while (true) {
            if (!endOfInput) {
                int nextCsPos = Math.min(csPos + inputBuffer.remaining(), cs.length());
                append(inputBuffer, cs, csPos, nextCsPos);
                inputBuffer.flip();
                endOfInput = nextCsPos == cs.length();
                csPos = nextCsPos;
            }

            CoderResult cr = inputBuffer.hasRemaining() ?
                    charsetEncoder.encode(inputBuffer, outputBuffer, endOfInput) :
                    CoderResult.UNDERFLOW;

            if (cr.isUnderflow() && endOfInput)
                cr = charsetEncoder.flush(outputBuffer);

            if (cr.isUnderflow()) {
                if (endOfInput) {
                    break;
                } else {
                    inputBuffer.compact();
                    continue;
                }
            }

            if (cr.isOverflow()) {
                outputBuffer.flip();
                writeOutputBuffer(out);
                outputBuffer.clear();
                continue;
            }

            try {
                cr.throwException();
            } catch (CharacterCodingException e) {
                throw new IORuntimeException(e);
            }
        }
        outputBuffer.flip();
        writeOutputBuffer(out);

        out.writeInt(encodedSizePos, (int) (out.writePosition() - encodedSizePos - 4));
    }

    private void writeOutputBuffer(Bytes out) {
        int remaining = outputBuffer.remaining();
        out.write(out.writePosition(), outputBuffer, 0, remaining);
        out.writeSkip(remaining);
    }

    /**
     * Need this method because {@link CharBuffer#append(CharSequence, int, int)} produces garbage
     */
    private static void append(CharBuffer charBuffer, CharSequence cs, int start, int end) {
        for (int i = start; i < end; i++) {
            charBuffer.put(cs.charAt(i));
        }
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        charset = (Charset) wireIn.read(() -> "charset").object();
        inputBufferSize = wireIn.read(() -> "inputBufferSize").int32();
        initTransients();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        wireOut.write(() -> "charset").object(charset);
        wireOut.write(() -> "inputBufferSize").int32(inputBufferSize);
    }

    @Override
    public CharSequenceCustomEncodingBytesWriter copy() {
        return new CharSequenceCustomEncodingBytesWriter(charset, inputBufferSize);
    }
}
```

Reader:

```java
public final class CharSequenceCustomEncodingBytesReader
        implements BytesReader<CharSequence>,
        StatefulCopyable<CharSequenceCustomEncodingBytesReader> {

    // config fields, non-final because read in readMarshallable()
    private Charset charset;
    private int inputBufferSize;

    // cache fields
    private transient CharsetDecoder charsetDecoder;
    private transient ByteBuffer inputBuffer;
    private transient CharBuffer outputBuffer;

    public CharSequenceCustomEncodingBytesReader(Charset charset, int inputBufferSize) {
        this.charset = charset;
        this.inputBufferSize = inputBufferSize;
        initTransients();
    }

    private void initTransients() {
        charsetDecoder = charset.newDecoder();
        inputBuffer = ByteBuffer.allocate(inputBufferSize);
        int outputBufferSize = (int) (inputBufferSize * charsetDecoder.averageCharsPerByte());
        outputBuffer = CharBuffer.allocate(outputBufferSize);
    }

    @NotNull
    @Override
    public CharSequence read(Bytes in, @Nullable CharSequence using) {
        long csLengthAsLong = in.readStopBit();
        if (csLengthAsLong > Integer.MAX_VALUE) {
            throw new IORuntimeException("cs len shouldn't be more than " + Integer.MAX_VALUE +
                    ", " + csLengthAsLong + " read");
        }
        int csLength = (int) csLengthAsLong;
        StringBuilder sb;
        if (using instanceof StringBuilder) {
            sb = (StringBuilder) using;
            sb.setLength(0);
            sb.ensureCapacity(csLength);
        } else {
            sb = new StringBuilder(csLength);
        }

        int remainingBytes = in.readInt();
        charsetDecoder.reset();
        inputBuffer.clear();
        outputBuffer.clear();
        boolean endOfInput = false;
        // this loop inspired by the CharsetDecoder.decode(ByteBuffer) implementation
        while (true) {
            if (!endOfInput) {
                int inputChunkSize = Math.min(inputBuffer.remaining(), remainingBytes);
                inputBuffer.limit(inputBuffer.position() + inputChunkSize);
                in.read(inputBuffer);
                inputBuffer.flip();
                remainingBytes -= inputChunkSize;
                endOfInput = remainingBytes == 0;
            }

            CoderResult cr = inputBuffer.hasRemaining() ?
                    charsetDecoder.decode(inputBuffer, outputBuffer, endOfInput) :
                    CoderResult.UNDERFLOW;

            if (cr.isUnderflow() && endOfInput)
                cr = charsetDecoder.flush(outputBuffer);

            if (cr.isUnderflow()) {
                if (endOfInput) {
                    break;
                } else {
                    inputBuffer.compact();
                    continue;
                }
            }

            if (cr.isOverflow()) {
                outputBuffer.flip();
                sb.append(outputBuffer);
                outputBuffer.clear();
                continue;
            }

            try {
                cr.throwException();
            } catch (CharacterCodingException e) {
                throw new IORuntimeException(e);
            }
        }
        outputBuffer.flip();
        sb.append(outputBuffer);

        return sb;
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) throws IORuntimeException {
        charset = (Charset) wireIn.read(() -> "charset").object();
        inputBufferSize = wireIn.read(() -> "inputBufferSize").int32();
        initTransients();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        wireOut.write(() -> "charset").object(charset);
        wireOut.write(() -> "inputBufferSize").int32(inputBufferSize);
    }

    @Override
    public CharSequenceCustomEncodingBytesReader copy() {
        return new CharSequenceCustomEncodingBytesReader(charset, inputBufferSize);
    }
}
```

Usage example:

```java
Charset charset = Charset.forName("GBK");
int charBufferSize = 100;
int bytesBufferSize = 200;
CharSequenceCustomEncodingBytesWriter writer =
        new CharSequenceCustomEncodingBytesWriter(charset, charBufferSize);
CharSequenceCustomEncodingBytesReader reader =
        new CharSequenceCustomEncodingBytesReader(charset, bytesBufferSize);
try (ChronicleMap<String, CharSequence> englishToChinese = ChronicleMap
        .of(String.class, CharSequence.class)
        .valueMarshallers(reader, writer)
        .averageKey("hello")
        .averageValue("你好")
        .entries(10)
        .create()) {
    englishToChinese.put("hello", "你好");
    englishToChinese.put("bye", "再见");

    Assert.assertEquals("你好", englishToChinese.get("hello").toString());
    Assert.assertEquals("再见", englishToChinese.get("bye").toString());
}
```

Some notes on this case of custom serialization:

 - Both `CharSequenceCustomEncodingBytesWriter` and `CharSequenceCustomEncodingBytesReader` have
 configurations (charset and input buffer size), hence they are implemented as normal classes rather
 than classes with `private` constructors and a single `INSTANCE`.
 - Both writer and reader classes have some "cache" fields, their contents are mutated during
 writing and reading. That is why they have to implement `StatefulCopyable` interface. See
 [Understanding `StatefulCopyable`](#understanding-statefulcopyable) section for more infromation on
 this.

##### `SizedWriter` and `SizedReader`

This pair of interfaces is configured via `ChronicleMapBuilder.keyMarshallers()` or
`valueMarshallers()` for the key or value type of the map, respectively (overloaded methods, those
for [`BytesWriter` and `BytesReader`](#byteswriter-and-bytesreader) interfaces have the same names).

The main two methods to implement in `SizedWriter` interface:

 - `long size(@NotNull T toWrite);` returns the number of bytes, which is written by the subsequent
 `write()` method given the same `toWrite` instance of the serialized type.
 - `void write(Bytes out, long size, @NotNull T toWrite);` writes the given `toWrite` instance to
 the given `out` bytes sink. Additionally `size` is provided, which is the value computed for the
 same `toWrite` instance by calling `size()` method. Hence, when `SizedWriter` is used internally,
 `size()` method is always called before `write()`, caching logic in `SizedWriter` implementation
 may rely on this. This method should advance `writePosition()` of the given out `Bytes` exactly by
 the given `size` number of bytes.

`SizedReader`'s `T read(Bytes in, long size, @Nullable T using);` method is similar to the
corresponding `read()` method in `BytesReader` interface, except the size of the serialized object
is provided.

This pair of interfaces is suitable, if

 - The serialized form of the type effectively includes the size of the rest of the serialized form
 in the beginning,
 - But the type is not a plain sequence of bytes like `byte[]` or `ByteBuffer` (for those types
 [`DataAccess` and `SizedReader`](#dataaccess-and-sizedreader) pair of interfaces is a much better
 choice).

Examples of such types include lists and arrays of constant-sized elements.

Compared to `BytesWriter` and `BytesReader`, this pair of interfaces allows to share the information
about the serialized form between serialization logic and the Chronicle Map, saving a few bytes by
storing the serialization size only once rather than twice. This pair of interfaces is not much more
difficult to implement, but the gains are also not big.

Example: serializing lists of simple `Point` structures:

```java
public final class Point {

    public static Point of(double x, double y) {
        Point p = new Point();
        p.x = x;
        p.y = y;
        return p;
    }

    double x, y;
}
```

Serializer implementation:

```java
public final class PointListSizedMarshaller
        implements SizedReader<List<Point>>, SizedWriter<List<Point>>,
        ReadResolvable<PointListSizedMarshaller> {

    static final PointListSizedMarshaller INSTANCE = new PointListSizedMarshaller();

    private PointListSizedMarshaller() {}

    /** A point takes 16 bytes in serialized form: 8 bytes for both x and y value */
    private static final long ELEMENT_SIZE = 16;

    @Override
    public long size(@NotNull List<Point> toWrite) {
        return toWrite.size() * ELEMENT_SIZE;
    }

    @Override
    public void write(Bytes out, long size, @NotNull List<Point> toWrite) {
        toWrite.forEach(point -> {
            out.writeDouble(point.x);
            out.writeDouble(point.y);
        });
    }

    @NotNull
    @Override
    public List<Point> read(@NotNull Bytes in, long size, List<Point> using) {
        if (size % ELEMENT_SIZE != 0) {
            throw new IORuntimeException("Bytes size should be a multiple of " + ELEMENT_SIZE +
                    ", " + size + " read");
        }
        long listSizeAsLong = size / ELEMENT_SIZE;
        if (listSizeAsLong > Integer.MAX_VALUE) {
            throw new IORuntimeException("List size couldn't be more than " + Integer.MAX_VALUE +
                    ", " + listSizeAsLong + " read");
        }
        int listSize = (int) listSizeAsLong;
        if (using == null) {
            using = new ArrayList<>(listSize);
            for (int i = 0; i < listSize; i++) {
                using.add(null);
            }
        } else if (using.size() < listSize) {
            while (using.size() < listSize) {
                using.add(null);
            }
        } else if (using.size() > listSize) {
            using.subList(listSize, using.size()).clear();
        }
        for (int i = 0; i < listSize; i++) {
            Point point = using.get(i);
            if (point == null)
                using.set(i, point = new Point());
            point.x = in.readDouble();
            point.y = in.readDouble();
        }
        return using;
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        // no fields to write
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        // no fields to read
    }

    @Override
    public PointListSizedMarshaller readResolve() {
        return INSTANCE;
    }
}
```

Usage example:

```java
try (ChronicleMap<String, List<Point>> objects = ChronicleMap
        .of(String.class, (Class<List<Point>>) (Class) List.class)
        .averageKey("range")
        .valueMarshaller(PointListSizedMarshaller.INSTANCE)
        .averageValue(asList(of(0, 0), of(1, 1)))
        .entries(10)
        .create()) {
    objects.put("range", asList(of(0, 0), of(1, 1)));
    objects.put("square", asList(of(0, 0), of(0, 100), of(100, 100), of(100, 0)));

    Assert.assertEquals(2, objects.get("range").size());
    Assert.assertEquals(4, objects.get("square").size());
}
```

##### `DataAccess` and `SizedReader`

This pair of interfaces is configured via `ChronicleMapBuilder.keyReaderAndDataAccess()` or
`valueReaderAndDataAccess()` for the key or value type of the map, respectively.

The reader part, `SizedReader`, is the same as in [`SizedWriter` and
`SizedReader`](#sizedwriter-and-sizedreader) pair, so `DataAccess` is an "advanced" interface to
replace `SizedWriter`.

The main method in `DataAccess` is `Data<T> getData(@NotNull T instance)`, it returns a `Data`
accessor which is used to write "serialized" form of the instance to off-heap memory. `Data.size()`
on the returned `Data` object is used for the same purpose as `SizedWriter.size()` method in
[`SizedWriter` and `SizedReader`](#sizedwriter-and-sizedreader) pair interfaces. `Data.writeTo()`
is used instead of `SizedWriter.write()`.

`DataAccess` assumes that the `Data` object, returned from the `getData()` method is cached in some
way, that is why it also has `uninit()` method to clear references to the serialized object after
query operation to a Chronicle Map is over (to prevent memory leaks). This, in its turn, implies
that `DataAccess` implementation is stateful, therefore `DataAccess` is made a subinterface of
`StatefulCopyable` to force all `DataAccess` implementations to implement `StatefulCopyable` as
well. See [Understanding `StatefulCopyable`](#understanding-statefulcopyable) for more infromation
on this. If your `DataAccess` implementation is not actually stateful, it is free to return `this`
from `StatefulCopyable.copy()` method.

`DataAccess` interface is primarily intended for "serializing" objects that are already sequences
of bytes and in fact doesn't require serialization, like `byte[]`, `ByteBuffer`, arrays of Java
primitives. For such types of objects, `DataAccess` allows to bypass intermediate buffering, copying
data directly from objects to Chronicle Map's off-heap memory.

For example, look at the `DataAccess` implementation for `byte[]`:

```java
public final class ByteArrayDataAccess extends AbstractData<byte[]> implements DataAccess<byte[]> {

    /** Cache field */
    private transient HeapBytesStore<byte[]> bs;

    /** State field */
    private transient byte[] array;

    public ByteArrayDataAccess() {
        initTransients();
    }

    private void initTransients() {
        bs = HeapBytesStore.uninitialized();
    }

    @Override
    public RandomDataInput bytes() {
        return bs;
    }

    @Override
    public long offset() {
        return bs.start();
    }

    @Override
    public long size() {
        return bs.capacity();
    }

    @Override
    public byte[] get() {
        return array;
    }

    @Override
    public byte[] getUsing(@Nullable byte[] using) {
        if (using == null || using.length != array.length)
            using = new byte[array.length];
        System.arraycopy(array, 0, using, 0, array.length);
        return using;
    }

    @Override
    public Data<byte[]> getData(@NotNull byte[] instance) {
        array = instance;
        bs.init(instance);
        return this;
    }

    @Override
    public void uninit() {
        array = null;
        bs.uninit();
    }

    @Override
    public DataAccess<byte[]> copy() {
        return new ByteArrayDataAccess();
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        // no fields to write
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        // no fields to read
        initTransients();
    }
}
```

Note that `getData()` method returns `this`, and the `DataAccess` implementation implements `Data`
interface as well. This is a recommended practice, because it reduces the number of objects involved
(hence pointer chasing), and keeps `DataAccess` and `Data` logic together.

`Data` interface puts constrains on `equals()`, `hashCode()` and `toString()` implementations, this
is why `ByteArrayDataAccess` subclasses `AbstractData` and inherits proper implementations from it.
This is OK to serializer strategy implementation to have `equals()`, `hashCode()` and `toString()`
from a very different domain, because those methods are never called on serializers inside Chronicle
Map.

The easiest way to implement `equals()`, `hashCode()` and `toString()` is to extend `AbstractData`
class, if it is not possible (the `Data` implementation already extends some other class), do this
by delegating to `dataEquals()`, `dataHashCode()` and `dataToString()` default methods, provided
right in `Data` interface.

Corresponding `SizedReader` for `byte[]`:

```java
public final class ByteArraySizedReader
        implements SizedReader<byte[]>, Marshallable, ReadResolvable<ByteArraySizedReader> {

    public static final ByteArraySizedReader INSTANCE = new ByteArraySizedReader();

    private ByteArraySizedReader() {}

    @NotNull
    @Override
    public byte[] read(@NotNull Bytes in, long size, @Nullable byte[] using) {
        if (size < 0L || size > (long) Integer.MAX_VALUE) {
            throw new IORuntimeException("byte[] size should be non-negative int, " +
                    size + " given. Memory corruption?");
        }
        int arrayLength = (int) size;
        if (using == null || arrayLength != using.length)
            using = new byte[arrayLength];
        in.read(using);
        return using;
    }

    @Override
    public void writeMarshallable(@NotNull WireOut wireOut) {
        // no fields to write
    }

    @Override
    public void readMarshallable(@NotNull WireIn wireIn) {
        // no fields to read
    }

    @Override
    public ByteArraySizedReader readResolve() {
        return INSTANCE;
    }
}
```

(Nothing is required to use this pair of serializers with `byte[]` keys of values in Chronicle Map,
it is used by default, if you configure `byte[]` key or value type.)

##### Understanding `StatefulCopyable`

**Problems:**

 1. Sometimes on writing, reading or in other methods, defined in serialization interfaces, it is
 needed to operate with intermediate objects, holding some writing/reading state.

 In the simplest and most common case, those objects are of some kind of buffers between serialized
 or deserialized instances and output or input `Bytes`: for example, see writer and reader
 implementations in the [custom `CharSequence` encoding](#custom-charsequence-encoding) section.

 To avoid producing a lot of garbage, those intermediate objects should be cached.

 2. `Data` object, returned from `getData()` method of `DataAccess` serialization interface, should
 be cached in order to avoid producing garbage.

 3. `SizedWriter` and `DataAccess` serialization interfaces, and the `Data` object, returned from
 `DataAccess.getData()` method, since it is cached (see the previous point) have several methods:
 `size()` and `write()` in `SizedWriter`, `getData()` and `uninit()` in `DataAccess`, many methods
 in `Data`. It is usually essential to save some computation results between calls to those methods
 from inside Chronicle Map implementation, while working some key or value during some query to
 Chronicle Map, because otherwise expensive computations are performed several times, that is very
 inefficient.

All these problems require to access some mutable, context-dependant fields from within serializer
interface implementations. But, only a *single instance* of serializer implementation is configured
for a Chronicle Map (either by `keyMarshallers()` or `valueMarshallers()` or
`keyReaderAndDataAccess()` or `valueReaderAndDataAccess()` method in `ChronicleMapBuilder`). On the
other hand, `ChronicleMap` is a `ConcurrentMap`, i. e. serializer implementations could be accessed
from multiple threads concurrently. Moreover, a single `ChronicleMapBuilder` (with a single pair
of serializer instances configured for keys and values) could be used to construct many independent
`ChronicleMap`s, which could also be accessed concurrently.

Inefficient and fragile solution: a single instance of serialization interface, static `ThreadLocal`
fields in serializer implementation class.

It is inefficient, because `ThreadLocal`s are accessed each time a serializer interface method is
called, that is much slower than accessing vanilla object fields.

It is fragile, because if you need `ThreadLocal` fields for preserving some state between calls to
multiple methods in serializer interfaces over a single query to a Chronicle Map (the 3rd point in
the problems list above), you should consider that a single serializer instance could be used for
both keys and values of the same Chronicle Map (calls to some methods of serializer interface for
serializing the key and the value over a Chronicle Map query are interspersed in unspecified order).
Plus, you should consider that a single serializer instance could be used to access multiple
independent Chronicle Map instances in the same thread, calls to serializers within maps could be
interspersed via [contexts access](#working-with-an-entry-within-a-context-section). So,
`ThreadLocal` fields should be isolated not just per accessing thread, but also per serialized
object domain (is the serialized object a key or a value in Chronicle Map), and per accessing
Chronicle Map instance, that is hard to implement correctly.

**Recommended solution: `StatefulCopyable`.** A serializer implementation has ordinary instance
fields for caching anything and preserving state between calls to different methods. It implements
`StatefulCopyable` interface with a single method `copy()`, that is like `Object.clone()`, but
copies only configuration fields of the serializer instance, not cache or state fields.

Call to `copy()` method at any point of a serializer instance lifetime should return an instance of
the same serializer implementation in the state, exactly equal to the state of the current instance
at the moment after construction and initial configuration. I. e. with "empty" cache and state.
As a consequence, `copy()` method is transitive, i. e. `serializer.copy().copy().copy()` should
return an object in exactly the same state, as after a single `copy()` call.

Chronicle Map implementation recognizes that configured `BytesWriter`, `BytesReader`, `SizedWriter`,
`SizedReader` or `DataAccess` instance implements `StatefulCopyable`, and "populates" it internally
via `copy()` for each thread, Chronicle Map instance and serialized object domain (keys or values).

See examples of implementing this interface in the [custom `CharSequence`
encoding](#custom-charsequence-encoding) section above.

It is allowed to return `this` from the `copy()` method, if with some configurations the
serializer implementation doesn't have state. Typically this is the case when serializer is
configured with sub-serializers which might be `StatefulCopyable` or not, for example
[`ListMarshaller`](src/main/java/net/openhft/chronicle/hash/serialization/ListMarshaller.java)
class.

##### Custom serialization checklist

 1. Choose the most suitable pair of serialization interfaces: [`BytesWriter` and
 `BytesReader`](#byteswriter-and-bytesreader), [`SizedWriter` and
 `SizedReader`](#sizedwriter-and-sizedreader) or [`DataAccess` and
 `SizedReader`](#dataaccess-and-sizedreader). Recommendations on which pair to choose are given in
 the linked sections, describing each pair.
 2. If implementation of the writer or reader part is configuration-less, give it a `private`
 constructor and define a single `INSTANCE` constant -- a sole instance of this marshaller class in
 the JVM. Implement `ReadResolvable` and return `INSTANCE` from `readResolve()` method. *But, don't
 make implementation a Java `enum`.*
 3. If both the writer and reader are configuration-less, merge them into a single `-Marshaller`
 implementation class.
 4. Make best effort in reusing `using` objects on the reader side (`BytesReader` or `SizedReader`),
 including nesting objects.
 5. Make best effort in caching intermediate serialization results on writer side while working with
 some object, e. g. try not to make expensive computations in both `size()` and `write()` methods
 of `SizedWriter` implementation, but rather lazily compute them and cache in an serializer instance
 field.
 6. Make best effort in reusing intermediate objects, used for reading or writing. Store them in
 instance fields of serializer implementation.
 7. If a serializer implementation is stateful or have cache fields, implement `StatefulCopyable`.
 See [Understanding `StatefulCopyable`](#understanding-statefulcopyable) section for more info.
 8. Implement `writeMarshallable()` and `readMarshallable()` by writing and reading configuration
 fields (but not state or cache fields) of the serializer instance one-by-one, using the given
 `WireOut`/`WireIn` object. See [Custom `CharSequence` encoding](#custom-charsequence-encoding)
 section for some non-trivial example of implementing these methods. See also [Wire tutorial](
 https://github.com/OpenHFT/Chronicle-Wire#using-wire).
 9. *Don't forget to initialize transient/cache/state fileds of the instance in the end of
 `readMarshallable()` implementation.* This is needed, because fefore calling `readMarshallable()`,
 Wire framework creates a serializer instance by means of `Unsafe.allocateInstance()` rather than
 calling any constructor.
 10. If implementing `DataAccess`, consider implementation to be `Data` also, and return `this` from
 `getData()` method.
 11. Don't forget to implement `equals()`, `hashCode()` and `toString()` in `Data` implementation,
 returned from `DataAccess.getData()` method, regardless if this is actually the same `DataAccess`
 object, or a separate object.
 12. Except `DataAccess` which is also a `Data`, serializers shouldn't override Object's `equals()`,
 `hashCode()` and `toString()` (these methods are never called on serializers inside Chronicle Map
 library); they shouldn't implement `Serializable` or `Externalizable` (but have to implement
 `net.openhft.chronicle.wire.Marshallable`); shouldn't implement `Cloneable` (but have to implement
 `StatefulCopyable`, if they are stateful or have cache fields).
 13. After implementing custom serializers, don't forget to actually apply them to
 `ChronicleMapBuilder` by `keyMarshallers()`, `keyReaderAndDataAccess()`, `valueMarshallers()` or
 `valueReaderAndDataAccess()` methods.

### `ChronicleMap` usage patterns

#### Single-key queries

First of all, `ChronicleMap` supports all operations from [`Map`](
https://docs.oracle.com/javase/8/docs/api/java/util/Map.html): `get()`, `put()`, etc, including
methods added in Java 8, like `compute()` and `merge()`, and [`ConcurrentMap`](
https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentMap.html) interfaces:
`putIfAbsent()`, `replace()`. All operations, including those which include "two steps", e. g.
`compute()`, are correctly synchronized in terms of `ConcurrentMap` interface.

This means, you could use `ChronicleMap` instance just like a `HashMap` or `ConcurrentHashMap`:

```java
PostalCodeRange amsterdamCodes = Values.newHeapInstance(PostalCodeRange.class);
amsterdamCodes.minCode(1011);
amsterdamCodes.maxCode(1183);
cityPostalCodes.put("Amsterdam", amsterdamCodes);

...

PostalCodeRange amsterdamCodes = cityPostalCodes.get("Amsterdam");
```

However, this approach often generates garbage, because the values should be deserialized from
off-heap memory to on-heap, the new value object are allocated. There are several possibilities to
reuse objects efficiently:

##### Value interfaces instead of boxed primitives

If you want to create a `ChronicleMap` where keys are `long` ids, use `LongValue` instead of `Long`
key:

```java
ChronicleMap<LongValue, Order> orders = ChronicleMap
    .of(LongValue.class, Order.class)
    .name("orders-map")
    .entries(1_000_000)
    .create();

LongValue key = Values.newHeapInstance(LongValue.class);
key.setValue(id);
orders.put(key, order);

...

long[] orderIds = ...
// Allocate a single heap instance for inserting all keys from the array.
// This could be a cached or ThreadLocal value as well, eliminating
// allocations altogether.
LongValue key = Values.newHeapInstance(LongValue.class);
for (long id : orderIds) {
    // Reuse the heap instance for each key
    key.setValue(id);
    Order order = orders.get(key);
    // process the order...
}
```

##### `chronicleMap.getUsing()`

Use `ChronicleMap#getUsing(K key, V using)` to reuse the value object. It works if:

 - The value type is `CharSequence`, pass `StringBuilder` as the `using` argument.
 For example:
 ```java
 ChronicleMap<LongValue, CharSequence> names = ...
 StringBuilder name = new StringBuilder();
 for (long id : ids) {
    key.setValue(id);
    names.getUsing(key, name);
    // process the name...
 }
 ```

 In this case, calling `names.getUsing(key, name)` is equivalent to
 ```java
 name.setLength(0);
 name.append(names.get(key));
 ```

 with the difference that it doesn't generate garbage.
 - The value type is value interface, pass heap instance to read the data into it without new object
 allocation:
 ```java
 ThreadLocal<PostalCodeRange> cachedPostalCodeRange =
    ThreadLocal.withInitial(() -> Values.newHeapInstance(PostalCodeRange.class));

 ...

 PostalCodeRange range = cachedPostalCodeRange.get();
 cityPostalCodes.getUsing(city, range);
 // process the range...
 ```
 - If the value type implements `BytesMarshallable`, or `Externalizable`, `ChronicleMap` attempts to
 reuse the given `using` object by deserializing the value into the given object.
 - If custom marshaller is configured in the `ChronicleMapBuilder` via `.valueMarshaller()`,
 `ChronicleMap` attempts to reuse the given object by calling `readUsing()` method from the
 marshaller interface.

If `ChronicleMap` fails to reuse the object in `getUsing()`, it makes no harm, it falls back to
object creation, like in `get()` method. In particular, even `null` is allowed to be passed as
`using` object. It allows "lazy" using object initialization pattern:
```java
// a field
PostalCodeRange cachedRange = null;

...

// in a method
cachedRange = cityPostalCodes.getUsing(city, cachedRange);
// process the range...
```
In this example, `cachedRange` is `null` initially, on the first `getUsing()` call the heap value
is allocated, and saved in a `cachedRange` field for later reuse.

<i>If the value type is a value interface, <b>don't</b> use flyweight implementation as `getUsing()`
argument.</i> This is dangerous, because on reusing flyweight points to the `ChronicleMap` memory
directly, but the access is not synchronized. At least you could read inconsistent value state,
at most - corrupt the `ChronicleMap` memory.

For accessing the `ChronicleMap` value memory directly use the following technique:

##### Working with an entry within a context section

```java
try (ExternalMapQueryContext<CharSequence, PostalCodeRange, ?> c =
        cityPostalCodes.queryContext("Amsterdam")) {
    MapEntry<CharSequence, PostalCodeRange> entry = c.entry();
    if (entry != null) {
        PostalCodeRange range = entry.value().get();
        // Access the off-heap memory directly, by calling range
        // object getters.
        // This is very rewarding, when the value has a lot of fields
        // and expensive to copy to heap all of them, when you need to access
        // just a few fields.
    } else {
        // city not found..
    }
}
```

#### Multi-key queries

In this example, consistent graph edge addition and removals are implemented via multi-key queries:
```java
public static boolean addEdge(
        ChronicleMap<Integer, Set<Integer>> graph, int source, int target) {
    if (source == target)
        throw new IllegalArgumentException("loops are forbidden");
    ExternalMapQueryContext<Integer, Set<Integer>, ?> sourceC = graph.queryContext(source);
    ExternalMapQueryContext<Integer, Set<Integer>, ?> targetC = graph.queryContext(target);
    // order for consistent lock acquisition => avoid dead lock
    if (sourceC.segmentIndex() <= targetC.segmentIndex()) {
        return innerAddEdge(source, sourceC, target, targetC);
    } else {
        return innerAddEdge(target, targetC, source, sourceC);
    }
}

private static boolean innerAddEdge(
        int source, ExternalMapQueryContext<Integer, Set<Integer>, ?> sourceContext,
        int target, ExternalMapQueryContext<Integer, Set<Integer>, ?> targetContext) {
    try (ExternalMapQueryContext<Integer, Set<Integer>, ?> sc = sourceContext) {
        try (ExternalMapQueryContext<Integer, Set<Integer>, ?> tc = targetContext) {
            sc.updateLock().lock();
            tc.updateLock().lock();
            MapEntry<Integer, Set<Integer>> sEntry = sc.entry();
            if (sEntry != null) {
                MapEntry<Integer, Set<Integer>> tEntry = tc.entry();
                if (tEntry != null) {
                    return addEdgeBothPresent(sc, sEntry, source, tc, tEntry, target);
                } else {
                    addEdgePresentAbsent(sc, sEntry, source, tc, target);
                    return true;
                }
            } else {
                MapEntry<Integer, Set<Integer>> tEntry = tc.entry();
                if (tEntry != null) {
                    addEdgePresentAbsent(tc, tEntry, target, sc, source);
                } else {
                    addEdgeBothAbsent(sc, source, tc, target);
                }
                return true;
            }
        }
    }
}

private static boolean addEdgeBothPresent(
        MapQueryContext<Integer, Set<Integer>, ?> sc,
        @NotNull MapEntry<Integer, Set<Integer>> sEntry, int source,
        MapQueryContext<Integer, Set<Integer>, ?> tc,
        @NotNull MapEntry<Integer, Set<Integer>> tEntry, int target) {
    Set<Integer> sNeighbours = sEntry.value().get();
    if (sNeighbours.add(target)) {
        Set<Integer> tNeighbours = tEntry.value().get();
        boolean added = tNeighbours.add(source);
        assert added;
        sEntry.doReplaceValue(sc.wrapValueAsData(sNeighbours));
        tEntry.doReplaceValue(tc.wrapValueAsData(tNeighbours));
        return true;
    } else {
        return false;
    }
}

private static void addEdgePresentAbsent(
        MapQueryContext<Integer, Set<Integer>, ?> sc,
        @NotNull MapEntry<Integer, Set<Integer>> sEntry, int source,
        MapQueryContext<Integer, Set<Integer>, ?> tc, int target) {
    Set<Integer> sNeighbours = sEntry.value().get();
    boolean added = sNeighbours.add(target);
    assert added;
    sEntry.doReplaceValue(sc.wrapValueAsData(sNeighbours));

    addEdgeOneSide(tc, source);
}

private static void addEdgeBothAbsent(MapQueryContext<Integer, Set<Integer>, ?> sc, int source,
        MapQueryContext<Integer, Set<Integer>, ?> tc, int target) {
    addEdgeOneSide(sc, target);
    addEdgeOneSide(tc, source);
}

private static void addEdgeOneSide(MapQueryContext<Integer, Set<Integer>, ?> tc, int source) {
    Set<Integer> tNeighbours = new HashSet<>();
    tNeighbours.add(source);
    MapAbsentEntry<Integer, Set<Integer>> tAbsentEntry = tc.absentEntry();
    assert tAbsentEntry != null;
    tAbsentEntry.doInsert(tc.wrapValueAsData(tNeighbours));
}

public static boolean removeEdge(
        ChronicleMap<Integer, Set<Integer>> graph, int source, int target) {
    ExternalMapQueryContext<Integer, Set<Integer>, ?> sourceC = graph.queryContext(source);
    ExternalMapQueryContext<Integer, Set<Integer>, ?> targetC = graph.queryContext(target);
    // order for consistent lock acquisition => avoid dead lock
    if (sourceC.segmentIndex() <= targetC.segmentIndex()) {
        return innerRemoveEdge(source, sourceC, target, targetC);
    } else {
        return innerRemoveEdge(target, targetC, source, sourceC);
    }
}

private static boolean innerRemoveEdge(
        int source, ExternalMapQueryContext<Integer, Set<Integer>, ?> sourceContext,
        int target, ExternalMapQueryContext<Integer, Set<Integer>, ?> targetContext) {
    try (ExternalMapQueryContext<Integer, Set<Integer>, ?> sc = sourceContext) {
        try (ExternalMapQueryContext<Integer, Set<Integer>, ?> tc = targetContext) {
            sc.updateLock().lock();
            MapEntry<Integer, Set<Integer>> sEntry = sc.entry();
            if (sEntry == null)
                return false;
            Set<Integer> sNeighbours = sEntry.value().get();
            if (!sNeighbours.remove(target))
                return false;

            tc.updateLock().lock();
            MapEntry<Integer, Set<Integer>> tEntry = tc.entry();
            if (tEntry == null)
                throw new IllegalStateException("target node should be present in the graph");
            Set<Integer> tNeighbours = tEntry.value().get();
            if (!tNeighbours.remove(source))
                throw new IllegalStateException("the target node have an edge to the source");
            sEntry.doReplaceValue(sc.wrapValueAsData(sNeighbours));
            tEntry.doReplaceValue(tc.wrapValueAsData(tNeighbours));
            return true;
        }
    }
}
```

Usage:
```java
HashSet<Integer> averageValue = new HashSet<>();
for (int i = 0; i < AVERAGE_CONNECTIVITY; i++) {
    averageValue.add(i);
}
ChronicleMap<Integer, Set<Integer>> graph = ChronicleMapBuilder
        .of(Integer.class, (Class<Set<Integer>>) (Class) Set.class)
        .name("graph")
        .entries(100)
        .averageValue(averageValue)
        .create();

addEdge(graph, 1, 2);
removeEdge(graph, 1, 2);
```

### Close `ChronicleMap`
Unlike ConcurrentHashMap, ChronicleMap stores its data off heap, often in a memory mapped file.
Its recommended that you call close() once you have finished working with a ChronicleMap.

``` java
map.close()
```

This is especially important when working with ChronicleMap replication, as failure to call close may prevent
you from restarting a replicated map on the same port. In the event that your application crashes it may not
be possible to call close(). Your operating system will usually close dangling ports automatically,
so although it is recommended that you close() when you have finished with the map,
its not something that you must do, it's just something that we recommend you should do.

###### WARNING

If you call close() too early before you have finished working with the map, this can cause
your JVM to crash. Close MUST BE the last thing that you do with the map.

### Behaviour Customization

You could customize `ChronicleMap` behaviour on several levels:

 - `ChronicleMapBuilder.entryOperations()` define the "inner" listening level, all operations with
 entries, either during ordinary map method calls, remote calls, replication or modifications during
 iteration over the map, operate via this configured SPI.

 - `ChronicleMapBuilder.mapMethods()` is the higher-level of listening for local calls of Map
 methods. Methods in `MapMethods` interface correspond to `Map` interface methods with the same
 names, and define their implementations for `ChronicleMap`.

 - `ChronicleMapBuilder.remoteOperations()` is for listening and customizing behaviour of remote
 calls, and replication events.

All executions around `ChronicleMap` go through the three tiers (or the two bottom):

 1. Query tier: `MapQueryContext` interface
 2. Entry tier: `MapEntry` and `MapAbsentEntry` interfaces
 3. Data tier: `Data` interface

`MapMethods` and `MapRemoteOperations` methods accept *query context*, i. e. these SPI is above
the Query tier. `MapEntryOperations` methods accept `MapEntry` or `MapAbsentEntry`, i. e. this SPI
is between Query and Entry tiers.

Combined, interception SPI interfaces and `ChronicleMap.queryContext()` API are powerful enough to

 - Log all operations of some kind on `ChronicleMap` (e. g. all remove, insert or update operations)
 - Log some specific operations on `ChronicleMap` (e. g. log only acquireUsing() calls, which has
 created a new entry)
 - Forbid performing operations of some kind on the `ChronicleMap` instance
 - Backup all changes to `ChronicleMap` to some alternative storage, e. g. SQL database
 - Perform multi-Chronicle Map operations correctly in concurrent environment, by acquiring locks on
 all ChronicleMaps before updating them.
 - Perform multi-key operations on a single `ChronicleMap` correctly in concurrent environment, by
 acquiring locks on all keys before updating the entries
 - Define own replication/reconciliation logic for distributed Chronicle Maps
 - Dump statistics of the Chronicle Map instance -- each segment's load, size in bytes of each
 entry, etc.

#### Example - Simple logging

Just log all modification operations on `ChronicleMap`

```java
class SimpleLoggingMapEntryOperations<K, V> implements MapEntryOperations<K, V, Void> {

    private static final SimpleLoggingMapEntryOperations INSTANCE =
            new SimpleLoggingMapEntryOperations();

    public static <K, V> MapEntryOperations<K, V, Void> simpleLoggingMapEntryOperations() {
        return SimpleLoggingMapEntryOperations.INSTANCE;
    }

    private SimpleLoggingMapEntryOperations() {}

    @Override
    public Void remove(@NotNull MapEntry<K, V> entry) {
        System.out.println("remove " + entry.key() + ": " + entry.value());
        entry.doRemove();
        return null;
    }

    @Override
    public Void replaceValue(@NotNull MapEntry<K, V> entry, Data<V, ?> newValue) {
        System.out.println("replace " + entry.key() + ": " + entry.value() + " -> " + newValue);
        entry.doReplaceValue(newValue);
        return null;
    }

    @Override
    public Void insert(@NotNull MapAbsentEntry<K, V> absentEntry, Data<V, ?> value) {
        System.out.println("insert " + absentEntry.absentKey() + " -> " + value);
        absentEntry.doInsert(value);
        return null;
    }
}
```

Usage:

```java
ChronicleMap<IntValue, IntValue> map = ChronicleMap
        .of(Integer.class, IntValue.class)
        .entries(100)
        .entryOperations(simpleLoggingMapEntryOperations())
        .create();

// do anything with the map
```

#### Example - BiMap

Possible bidirectional map (i. e. a map that preserves the uniqueness of its values as well
as that of its keys) implementation over Chronicle Maps.

```java
enum DualLockSuccess {SUCCESS, FAIL}
```

```java
class BiMapMethods<K, V> implements MapMethods<K, V, DualLockSuccess> {
    @Override
    public void remove(MapQueryContext<K, V, DualLockSuccess> q, ReturnValue<V> returnValue) {
        while (true) {
            q.updateLock().lock();
            try {
                MapEntry<K, V> entry = q.entry();
                if (entry != null) {
                    returnValue.returnValue(entry.value());
                    if (q.remove(entry) == SUCCESS)
                        return;
                }
            } finally {
                q.readLock().unlock();
            }
        }
    }

    @Override
    public void put(MapQueryContext<K, V, DualLockSuccess> q, Data<V, ?> value,
                    ReturnValue<V> returnValue) {
        while (true) {
            q.updateLock().lock();
            try {
                MapEntry<K, V> entry = q.entry();
                if (entry != null) {
                    throw new IllegalStateException();
                } else {
                    if (q.insert(q.absentEntry(), value) == SUCCESS)
                        return;
                }
            } finally {
                q.readLock().unlock();
            }
        }
    }

    @Override
    public void putIfAbsent(MapQueryContext<K, V, DualLockSuccess> q, Data<V, ?> value,
                            ReturnValue<V> returnValue) {
        while (true) {
            try {
                if (q.readLock().tryLock()) {
                    MapEntry<?, V> entry = q.entry();
                    if (entry != null) {
                        returnValue.returnValue(entry.value());
                        return;
                    }
                    // Key is absent
                    q.readLock().unlock();
                }
                q.updateLock().lock();
                MapEntry<?, V> entry = q.entry();
                if (entry != null) {
                    returnValue.returnValue(entry.value());
                    return;
                }
                // Key is absent
                if (q.insert(q.absentEntry(), value) == SUCCESS)
                    return;
            } finally {
                q.readLock().unlock();
            }
        }
    }

    @Override
    public boolean remove(MapQueryContext<K, V, DualLockSuccess> q, Data<V, ?> value) {
        while (true) {
            q.updateLock().lock();
            MapEntry<K, V> entry = q.entry();
            try {
                if (entry != null && bytesEquivalent(entry.value(), value)) {
                    if (q.remove(entry) == SUCCESS) {
                        return true;
                    } else {
                        //noinspection UnnecessaryContinue
                        continue;
                    }
                } else {
                    return false;
                }
            } finally {
                q.readLock().unlock();
            }
        }
    }

    @Override
    public void acquireUsing(MapQueryContext<K, V, DualLockSuccess> q,
                             ReturnValue<V> returnValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replace(MapQueryContext<K, V, DualLockSuccess> q, Data<V, ?> value,
                        ReturnValue<V> returnValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(MapQueryContext<K, V, DualLockSuccess> q, Data<V, ?> oldValue,
                           Data<V, ?> newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void compute(MapQueryContext<K, V, DualLockSuccess> q,
                        BiFunction<? super K, ? super V, ? extends V> remappingFunction,
                        ReturnValue<V> returnValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void merge(MapQueryContext<K, V, DualLockSuccess> q, Data<V, ?> value,
                      BiFunction<? super V, ? super V, ? extends V> remappingFunction,
                      ReturnValue<V> returnValue) {
        throw new UnsupportedOperationException();
    }
}
```

```java
class BiMapEntryOperations<K, V> implements MapEntryOperations<K, V, DualLockSuccess> {
    ChronicleMap<V, K> reverse;

    public void setReverse(ChronicleMap<V, K> reverse) {
        this.reverse = reverse;
    }

    @Override
    public DualLockSuccess remove(@NotNull MapEntry<K, V> entry) {
        try (ExternalMapQueryContext<V, K, ?> rq = reverse.queryContext(entry.value())) {
            if (!rq.updateLock().tryLock()) {
                if (entry.context() instanceof MapQueryContext)
                    return FAIL;
                throw new IllegalStateException("Concurrent modifications to reverse map " +
                        "during remove during iteration");
            }
            MapEntry<V, K> reverseEntry = rq.entry();
            if (reverseEntry != null) {
                entry.doRemove();
                reverseEntry.doRemove();
                return SUCCESS;
            } else {
                throw new IllegalStateException(entry.key() + " maps to " + entry.value() +
                        ", but in the reverse map this value is absent");
            }
        }
    }

    @Override
    public DualLockSuccess replaceValue(@NotNull MapEntry<K, V> entry, Data<V, ?> newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DualLockSuccess insert(@NotNull MapAbsentEntry<K, V> absentEntry,
                                  Data<V, ?> value) {
        try (ExternalMapQueryContext<V, K, ?> rq = reverse.queryContext(value)) {
            if (!rq.updateLock().tryLock())
                return FAIL;
            MapAbsentEntry<V, K> reverseAbsentEntry = rq.absentEntry();
            if (reverseAbsentEntry != null) {
                absentEntry.doInsert(value);
                reverseAbsentEntry.doInsert(absentEntry.absentKey());
                return SUCCESS;
            } else {
                Data<K, ?> reverseKey = rq.entry().value();
                if (reverseKey.equals(absentEntry.absentKey())) {
                    // recover
                    absentEntry.doInsert(value);
                    return SUCCESS;
                }
                throw new IllegalArgumentException("Try to associate " +
                        absentEntry.absentKey() + " with " + value + ", but in the reverse " +
                        "map this value already maps to " + reverseKey);
            }
        }
    }
}
```

Usage:
```java
BiMapEntryOperations<Integer, CharSequence> biMapOps1 = new BiMapEntryOperations<>();
ChronicleMap<Integer, CharSequence> map1 = ChronicleMapBuilder
        .of(Integer.class, CharSequence.class)
        .name("direct-bimap")
        .entries(100)
        .actualSegments(1)
        .averageValueSize(10)
        .entryOperations(biMapOps1)
        .mapMethods(new BiMapMethods<>())
        .create();

BiMapEntryOperations<CharSequence, Integer> biMapOps2 = new BiMapEntryOperations<>();
ChronicleMap<CharSequence, Integer> map2 = ChronicleMapBuilder
        .of(CharSequence.class, Integer.class)
        .name("reverse-bimap")
        .entries(100)
        .actualSegments(1)
        .averageKeySize(10)
        .entryOperations(biMapOps2)
        .mapMethods(new BiMapMethods<>())
        .create();

biMapOps1.setReverse(map2);
biMapOps2.setReverse(map1);

map1.put(1, "1");
System.out.println(map2.get("1"));
```

#### Example - Monitor Chronicle Map Statistics

```java
    public static <K, V> void printMapStats(ChronicleMap<K, V> map) {
        for (int i = 0; i < map.segments(); i++) {
            try (MapSegmentContext<K, V, ?> c = map.segmentContext(i)) {
                System.out.printf("segment %d contains %d entries\n", i, c.size());
                c.forEachSegmentEntry(e -> System.out.printf("%s, %d bytes -> %s, %d bytes\n",
                        e.key(), e.key().size(), e.value(), e.value().size()));
            }
        }
    }
```

### Entry checksums

Chronicle Map 3 is able to store entry checksums along with entries. With entry checksums it is
possible to identify partially written entries (in case of operating system or power failure while)
and corrupted entries (in case of hardware memory or disk corruption) and clean them up during
[recovery](#recovery) procedure.

Entry checksums are 32-bit numbers, computed by a hash function with good avalanche effect.
Theoretically there is still about a one-billionth chance that after entry corruption it passes the
sum check.

By default, entry checksums are ON if the Chronicle Map is persisted to disk (i. e. created via
`createPersistedTo()` method), and OFF if the Chronicle Map is purely in-memory. Storing checksums
for a purely in-memory Chronicle Map hardly makes any practical sense, but you might want to disable
storing checksums for a persisted Chronicle Map by calling `.checksumEntries(false)` on the
`ChronicleMapBuilder` used to create a map. It makes sense if you don't need extra safety checksums
provide.

Entry checksums are computed automatically when an entry is inserted into a Chronicle Map, and
re-computed automatically on operations which update the whole value, e. g. `map.put()`,
`map.replace()`, `map.compute()`, `mapEntry.doReplaceValue()` (See `MapEntry` interface in
[Javadocs](http://www.javadoc.io/doc/net.openhft/chronicle-map/). But if you update values directly,
bypassing Chronicle Map logic, keeping entry checksum up-to-date is also your responsibility.

It is strongly recommended to update off-heap memory of values directly only within a
[context](#working-with-an-entry-within-a-context-section) and update or write lock held. Within a
context, you are provided with an *entry* object of `MapEntry` type. To re-compute entry checksum
manually, cast that object to `ChecksumEntry` type and call `.updateChecksum()` method on it:

```java
try (ChronicleMap<Integer, LongValue> map = ChronicleMap
        .of(Integer.class, LongValue.class)
        .entries(1)
        // Entry checksums make sense only for persisted Chronicle Maps, and are ON by
        // default for such maps
        .createPersistedTo(file)) {

    LongValue value = Values.newHeapInstance(LongValue.class);
    value.setValue(42);
    map.put(1, value);

    try (ExternalMapQueryContext<Integer, LongValue, ?> c = map.queryContext(1)) {
        // Update lock required for calling ChecksumEntry.checkSum()
        c.updateLock().lock();
        MapEntry<Integer, LongValue> entry = c.entry();
        Assert.assertNotNull(entry);
        ChecksumEntry checksumEntry = (ChecksumEntry) entry;
        Assert.assertTrue(checksumEntry.checkSum());

        // to access off-heap bytes, should call value().getUsing() with Native value
        // provided. Simple get() return Heap value by default
        LongValue nativeValue =
                entry.value().getUsing(Values.newNativeReference(LongValue.class));
        // This value bytes update bypass Chronicle Map internals, so checksum is not
        // updated automatically
        nativeValue.setValue(43);
        Assert.assertFalse(checksumEntry.checkSum());

        // Restore correct checksum
        checksumEntry.updateChecksum();
        Assert.assertTrue(checksumEntry.checkSum());
    }
}
```

## FAQ's

### Question

I am investigating Chronicle Map as a potential replacement of Redis - with concurrency in mind. In our architecture we would be looking to replace a "large" Redis instance that currently has multiple clients connecting to it causing latency pileups due to Redis' blocking nature.

The issue is that we need to make requests in random batches of ~1000. With Redis we are able to make a single request via a Lua script (or multi-get / multi-set commands) and receive a single response. In the documentation on Chronicle Maps stateless client I see that the remote calls are blocking and can be made only one key at a time, so for us the solution is not obvious.

While I am considering passing off each individual key task to a threadpool running X blocking threads at a time, I wonder if there might be a better solution that could take advantage of doing RPC in batches and perhaps work asynchronously. As I do not see this available currently, my questions are whether this is an enhancement you might consider or if you could perhaps point me to if/how we could write our own solution for doing this - which we'd be open to contributing back...

Also, is there a reason these 1000 gets have be done serially in one thread? Why not submit 1000 get() tasks to a pool of say 20 threads, shouldn't this improve throughput / reduce latency?

### Answer

stateless client is has been to be replaced with Engine. It is already not supported for 
ChronicleMap 3.x.  ( for the rest of the answer on this question please refer to Chronile Engine 
FAQ's )

For get()s, sure parallelizing will reduce costs. For put()s, if you have concurrency requirements, i. e. multi-key lock before updating all of them, of cause it should be in one thread.

The stateless client which was avaible in the last version gives better performance if you use a 
number
 of 
threads - 
see 
https://github.com/OpenHFT/Chronicle-Map#how-to-speed-up-the-chronicle-map-stateless-client <https://github.com/OpenHFT/Chronicle-Map#how-to-speed-up-the-chronicle-map-stateless-client>

I don’t see that you would gain a performance benefit, in using batches, unless you are compressing the batch of data. All the data will have to be sent via tcp anyway, even if its in a batch.

Note : under high load the chronicle map stateless client consolidates many small TCP request into a single request ( when run with a number of threads. )
