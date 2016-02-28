# Chronicle Map
<img align="right" src="http://openhft.net/wp-content/uploads/2014/07/ChronicleMap_200px.png">

<h4>Documentation: <a href="#chronicle-map-3-tutorial">Tutorial</a>,
<a href="http://www.javadoc.io/doc/net.openhft/chronicle-map/">Javadoc</a></h4>

<h4>Community support: <a href="https://github.com/OpenHFT/Chronicle-Map/issues">Issues</a>,
<a href="https://groups.google.com/forum/#!forum/java-chronicle">Chronicle
mailing list</a>, <a href="http://stackoverflow.com/tags/chronicle">Stackoverflow</a>,
<a href="https://plus.google.com/communities/111431452027706917722">Chronicle User's group</a></h4>

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
 - Multiple processes could access a Chronicle Map instance concurrently. At the same time,
 the instance is *in-process for each of the accessing processes*. (Out-of-process approach to IPC
 is simply incompatible with Chronicle Map's median latency target of < 1 μs.)

 - Replication *without logs*, with constant footprint cost, guarantees progress even if the network
 doesn't sustain write rates.

<b><i>Chronicle Map</i> has two meanings:</b> [the language-agnostic data store](spec) and [the
implementation of this data store for the JVM](src). Currently, this is the only implementation.

**From Java perspective,** Chronicle Map is a `ConcurrentMap` implementation which stores the
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
 - Durability - no, at most, Chronicle Map could be persisted to disk. **Durability with Chronicle
 Map is provided by another level of architecture,** for example all requests are sent to several
 nodes - master and hot standby. Clustering/distributed architecture is out of the scope of the
 Chronicle Map project, there are projects *on top* of Chronicle Map which address these questions,
 e. g. [Chronicle Enterprise](http://chronicle.software/products/chronicle-enterprise/).

**What is the Chronicle Map's data structure?** In one sentence and simplified, a Chronicle Map
instance is a big chunk of shared memory (optionally mapped to disk), split into independent
segments, each segment has an independent memory allocation for storing the entries, a hash table
for search, and a lock in shared memory (implemented via CAS loops) for managing concurrent access.
Read [the Chronicle Map data store design overview](spec/2-design-overview.md) for more.

### Chronicle Map is *not*

 - A document store. No secondary indexes.
 - A [multimap](https://en.wikipedia.org/wiki/Multimap). Using a `ChronicleMap<K, Collection<V>>`
 as multimap is technically possible, but often leads to problems. Developing a proper multimap with
 Chronicle Map's design principles is possible, [contact us](mailto:sales@chronicle.software) is
 you consider sponsoring such development.

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
    <th rowspan="5"><a href="http://chronicle.software/consultancy/">
    Closed-source,<br>on-demand</a></th>
  </tr>
  <tr>
    <td>Eventually-consistent replication (100% redundancy)</td>
  </tr>
  <tr>
    <td>Partially-redundant replication</td>
  </tr>
  <tr>
    <td>Synchronous replication</td>
  </tr>
  <tr>
    <td>Entry expiration timeouts</td>
  </tr>
</table>

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
   - [Key and Value Types](#key-and-value-types)
 - [`ChronicleMap` instance usage patterns](#chroniclemap-instance-usage-patterns)
   - [Single-key queries](#single-key-queries)
   - [Multi-key queries](#multi-key-queries)
 - [Close `ChronicleMap`](#close-chroniclemap)
 - [Behaviour Customization](#behaviour-customization)
   - [Example - Simple logging](#example---simple-logging)
   - [Example - BiMap](#example---bimap)
   - [Example - Monitor Chronicle Map statistics](#example---monitor-chronicle-map-statistics)

### Difference between Chronicle Map 2 and 3

Functional changes in Chronicle Map 3:

 - Added support for multi-key queries.
 - "Listeners" mechanism fully reworked, see the [Behaviour Customization](#behaviour-customization)
 section. This has a number of important consequences, most notable is:
   - Possibility to define replication eventual-consistency strategy, different from "last write
   wins", e. g. any state-based CRDT.
 - "Stateless clients" functionality (i. e. remote calls) is moved to [Chronicle Engine](
 https://github.com/OpenHFT/Chronicle-Engine).
 - Replication is done via [Chronicle Engine](https://github.com/OpenHFT/Chronicle-Engine).
 - Chronicle Map 2 has hard creation-time limit on the number of entries storable in the Chronicle
 Map instance. If the size exceeds this limit, an exception is thrown. In Chronicle Map 3, this
 limitation is removed, though the number of entries still has to be configured on the Chronicle Map
 instance creation, exceeding this configured limit is possible, but discouraged. See the
 [Number of entries configuration](#number-of-entries-configuration) section.
 - A number of smaller improvements and fixes.

Non-functional changes:

 - Chronicle Map 3 requires Java version 8 or newer, while Chronicle Map 2 supports Java 7.
 - Chronicle Map 3 has [specification](spec), [versioning policy](docs/versioning.md) and
 [compatibility policy](docs/compatibility.md). Chronicle Map 2 doesn't have such documents.

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

Creating an instance of ChronicleMap is a little more complex than just calling a constructor.
To create an instance you have to use the `ChronicleMapBuilder`.

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
        .averageKey("Amsterdam")
        .entries(50_000);
ChronicleMap<CharSequence, PostalCodeRange> cityPostalCodes =
    cityPostalCodesMapBuilder.create();

// Or shorter form, without builder variable extraction:

ChronicleMap<Integer, PostalCodeRange> cityPostalCodes = ChronicleMap
    .of(CharSequence.class, PostalCodeRange.class)
    .averageKey("Amsterdam")
    .entries(50_000)
    .create();
```

This snippet creates a `ChronicleMap`, supposed to store about 50 000 city name -> postal code
mappings. It is accessible within a single Java process - the process it is created within. The
data is accessible while the process is alive.

Replace `.create()` calls with `.createPersistedTo(cityPostalCodesFile)`, if you want the Chronicle
Map to either

 - Outlive the process it was created within, e. g. to support hot Java application redeploy
 - Be accessible from multiple processes on the same server
 - Persist the data to disk

The `cityPostalCodesFile` has to represent the same location on your server among all Java
processes, wishing to access this Chronicle Map instance, e. g.
`System.getProperty("java.io.tmpdir") + "/cityPostalCodes.dat"`.

The name and location of the file is entirely up to you.

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

**Single `ChronicleMap` instance per JVM.** If you want to access the Chronicle Map instance
concurrently within the Java process, you should *not* create a separate `ChronicleMap` instance per
thread. Within the JVM environment, `ChronicleMap` instance *is* a `ConcurrentMap`, and could be
accessed concurrently the same way as e. g. `ConcurrentHashMap`.

#### Key and Value Types

Either key or value type of `ChronicleMap<K, V>` could be:

 - Boxed primitive: `Integer`, `Long`, `Double`, etc.
 - `String` or `CharSequence`
 - Array of Java primitives, e. g. `byte[]`, `char[]` or `int[]`
 - Any type implementing `BytesMarshallable` from [Chronicle Bytes](
 https://github.com/OpenHFT/Chronicle-Bytes)
 - Any [value interface](https://github.com/OpenHFT/Chronicle-Values)
 - Any Java type implementing `Serializable` or `Externalizable` interface
 - *Any other type*, if `.keyMarshaller()` or `.valueMarshaller()` (for the key or value type
 respectively) is additionally configured in the `ChronicleMapBuilder`.

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
        // All UUIDs take 16 bytes.
        .constantKeySizeBySample(UUID.randomUUID())
        .entries(1_000_000)
        .create();
```

### `ChronicleMap` instance usage patterns

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

##### Working with the entry within a context section

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
 - Backup all changes to `ChronicleMap` to some durable storage, e. g. SQL database
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
        .entries(100)
        .actualSegments(1)
        .averageValueSize(10)
        .entryOperations(biMapOps1)
        .mapMethods(new BiMapMethods<>())
        .create();

BiMapEntryOperations<CharSequence, Integer> biMapOps2 = new BiMapEntryOperations<>();
ChronicleMap<CharSequence, Integer> map2 = ChronicleMapBuilder
        .of(CharSequence.class, Integer.class)
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
