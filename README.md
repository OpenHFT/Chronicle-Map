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
 - *Ultra low latency:* Chronicle Map targets median latency of both read and write queries of less
 than 1 *micro*second in [certain tests](
 https://github.com/OpenHFT/Chronicle-Map/search?l=java&q=perf&type=Code).
 - *High concurrency:* write queries scale well up to the number of hardware execution threads in
 the server. Read queries never block each other.
 - (Optional) persistence to disk.
 - (Optional) eventually-consistent, fully-redundant, asynchronous replication across servers,
 "last write wins" strategy by default, allows to implement custom [state-based CRDT](
 https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type) strategy.
 - Multi-key queries.

**Unique features**
 - Multiple processes could access a Chronicle Map instance concurrently. At the same time,
 the instance is *in-process for each of the accessing processes*. (Out-of-process approach to IPC
 is simply incompatible with Chronicle Map's median latency target of < 1 μs.)

 - Replication *without logs*, with constant footprint cost, guarantees progress even if the network
 doesn't sustain write rates.

<b><i>Chronicle Map</i> has two meanings:</b> [the language-agnostic data structure](
https://github.com/OpenHFT/Chronicle-Map/tree/master/spec) and [the implementation of this data
structure for the JVM](https://github.com/OpenHFT/Chronicle-Map/tree/master/src). Currently, this is
the only implementation.

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

**What is the Chronicle Map data structure?** In one sentence and simplified, Chronicle Map instance
is a big chunk of shared memory (optionally mapped to disk), split into independent segments, each
segment has an independent memory allocation for storing the entries, a hash table for search, and a
lock in shared memory (implemented via CAS loops) for managing concurrent access. Read [the
Chronicle Map design overview](
https://github.com/OpenHFT/Chronicle-Map/blob/master/spec/2-design-overview.md) for more.

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

Not supported out of the box in open-source version, but could be added in ad-hoc manner:
 - Partially-redundant replication, i. e. distributed/cluster solution on top of Chronicle Map
 - Synchronous replication
 - Entry expiration timeouts

[We could help to implement such things](http://chronicle.software/consultancy/).

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
 - [TCP / UDP Replication](#tcp--udp-replication)
   - [Multiple Processes on the same server with Replication](
   #multiple-processes-on-the-same-server-with-replication)
   - [Configuring Three Way TCP/IP Replication](#configuring-three-way-tcp--ip-replication)
 - [Multi Chronicle Maps - Network Distributed](#multiple-chronicle-maps---network-distributed)
 - [Behaviour Customization](#behaviour-customization)
   - [Example - Simple logging](#example---simple-logging)
   - [Example - BiMap](#example---bimap)
   - [Example - CRDT values for replicated Chronicle Maps - Grow-only set](
   #example---crdt-values-for-replicated-chronicle-maps---grow-only-set)

### Difference between Chronicle Map 2 and 3

Changes in Chronicle Map 3:

 - Added support for multi-key queries.
 - "Listeners" mechanism fully reworked, see the [Behaviour Customization](#behaviour-customization)
 section. This has a number of important consequences, most notable is:
   - Possibility to define replication eventual-consistency strategy, different from "last write
   wins", e. g. any state-based CRDT.
 - "Stateless clients" removed, access Chronicle Maps from remote servers via [Chronicle Engine](
 https://github.com/OpenHFT/Chronicle-Engine).
 - Chronicle Map 2 has hard creation-time limit on the number of entries storable in the Chronicle
 Map instance. If the size exceeds this limit, an exception is thrown. In Chronicle Map 3, this
 limitation is removed, though the number of entries still has to be configured on the Chronicle Map
 instance creation, exceeding this configured limit is possible, but discouraged. See the
 [Number of entries configuration](#number-of-entries-configuration) section.
 - A number of smaller improvements and fixes.

If you use Chronicle Map 2, you might be looking for [Chronicle Map 2 Tutorial](
https://github.com/OpenHFT/Chronicle-Map/tree/2.1#contents) or [Chronicle Map 2 Javadoc](
http://openhft.github.io/Chronicle-Map/apidocs/).

### Download the library

#### Maven Artifact Download
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
from:
```xml
https://oss.sonatype.org/content/repositories/snapshots/net/openhft/chronicle-map/
```
a better way is to add the following to your setting.xml, to allow maven to download snapshots:

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
`averageValueSizeBySample()`, instead of `averageKey()` or `averageValue()`, for example:
```java
ChronicleSet<UUID> uuids =
    ChronicleSetBuilder.of(UUID.class)
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
LongValue key = Values.newHeapInstance(LongValue.class);
for (long id : orderIds) {
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


### TCP / UDP Replication

ChronicleMap supports both TCP and UDP replication

![TCP/IP Replication](http://chronicle.software/wp-content/uploads/2014/07/Chronicle-Map-TCP-Replication_simple_02.jpg)

#### TCP / UDP Background.
TCP/IP is a reliable protocol, what this means is that unless you have a network failure or hardware
outage the data is guaranteed to arrive. TCP/IP provides point to point connectivity. So in effect
( over simplified ), if the message was sent to 100 hosts, the message would have to be sent
100 times. With UDP, the message is only sent once. This is ideal if you have a large number of
hosts and you wish to broadcast the same data to each of them.   However, one of the big drawbacks
with UDP is that it's not a reliable protocol. This means, if the UDP message is Broadcast onto
the network, the hosts are not guaranteed to receive it, so they can miss data. Some solutions
attempt to build resilience into UDP, but arguably, this is in effect reinventing TCP/IP.

#### How to setup UDP Replication
In reality on a good quality wired LAN, when using UDP, you will rarely miss messages. Nevertheless this is
a risk that we suggest you don't take. We suggest that whenever you use UDP replication you use it
in conjunction with a throttled TCP replication, therefore if a host misses a message over UDP, they
will later pick it up via TCP/IP. 

####  TCP/IP  Throttling
We are careful not to swamp your network with too much TCP/IP traffic, We do this by providing
a throttled version of TCP replication. This works because ChronicleMap only broadcasts the latest
update of each entry. 

#### Replication How it works

ChronicleMap provides multi master hash map replication. What this means, is that each remote
map, mirrors its changes over to another remote map, neither map is considered
the master store of data. Each map uses timestamps to reconcile changes.
We refer to in instance of a remote map as a node.
A node can be connected to up to 128 other nodes.
The data that is stored locally in each node becomes eventually consistent. So changes made to one
node, for example by calling put(), will be replicated over to the other node. To achieve a high
level of performance and throughput, the call to put() won’t block, 
With ConcurrentHashMap, It is typical to check the return code of some methods to obtain the old
value, for example remove(). Due to the loose coupling and lock free nature of this multi master
implementation,  this return value is only the old value on the nodes local data store. In other
words the nodes are only concurrent locally. Its worth realising that another node performing
exactly the same operation may return a different value. However reconciliation will ensure the maps
themselves become eventually consistent.

#### Reconciliation
If two ( or more nodes ) receive a change to their maps for the same key but different values, say
by a user of the maps, calling the put(key,value), then, initially each node will update its local
store and each local store will hold a different value. The aim of multi master replication is
to provide eventual consistency across the nodes. So, with multi master whenever a node is changed
it will notify the other nodes of its change. We will refer to this notification as an event.
The event will hold a timestamp indicating the time the change occurred, it will also hold the state
transition, in this case it was a put with a key and value.
Eventual consistency is achieved by looking at the timestamp from the remote node, if for a given
key, the remote nodes timestamp is newer than the local nodes timestamp, then the event from
the remote node will be applied to the local node, otherwise the event will be ignored. Since
none of the nodes is a primary, each node holds information about the other nodes. For this node its
own identifier is referred to as its 'localIdentifier', the identifiers of other nodes are the
'remoteIdentifiers'. On an update or insert of a key/value, this node pushes the information of
the change to the remote nodes. The nodes use non-blocking java NIO I/O and all replication is done
on a single thread. However there is an edge case. If two nodes update their map at the same time
with different values, we have to deterministically resolve which update wins. This is because eventual
consistency mandates that both nodes should end up locally holding the same data. Although it is rare that two remote
nodes receive an update to their maps at exactly the same time for the same key, we have
to handle this edge case.  We can not therefore rely on timestamps alone to reconcile
the updates. Typically the update with the newest timestamp should win, but in this example both
timestamps are the same, and the decision made to one node should be identical to the decision made
to the other. This dilemma is resolved by using a node identifier, the node identifier is a unique
'byte' value that is assigned to each node. When the time stamps are the same the remote node with the
smaller identifier will be preferred.

#### Multiple Processes on the same server with Replication

On a server if you have a number of Java processes and then within each Java process you create an instance of a ChronicleMap which binds to the same underline 'file', they exchange data via shared memory rather than TCP or UDP replication. So if a ChronicleMap which is not performing TCP Replication is updated, this update can be picked up by another ChronicleMap. This other ChronicleMap could be a TCP replicated ChronicleMap. In such an example the TCP replicated ChronicleMap would then push the update to the remote nodes.

Likewise, if the TCP replicated ChronicleMap was to received an update from a remote node, then this update would be immediately available to all the ChronicleMaps on the server.

### Identifier for Replication
If all you are doing is replicating your ChronicleMaps on the same server you don't have to set up
TCP and UDP replication. You also don't have to set the identifiers - as explained earlier this identifier is only for the resolution of conflicts amongst remote servers.

If however you wish to replicate data between 2 or more servers, then ALL of the ChronicleMaps
including those not actively participating in TCP or UDP replication must have the identifier set.
The identifier must be unique to each server. Each ChronicleMap on the same server must have
the same identifier. The reason that all ChronicleMaps must have the identifier set, is because
the memory is laid out slightly differently when using replication, so even if a Map is not actively
performing TCP or UDP replication itself, if it wishes to replicate with one that is, it must have
its memory laid out the same way to be compatible. 

If the identifiers are not set up uniquely then the updates will be ignored, as for example
a ChronicleMap set up with the identifiers equals '1', will ignore all events which contain
the remote identifier of '1', in other words Chronicle Map replication is set up to ignore updates
which have originated from itself. This is to avoid the circularity of events.

When setting up the identifier you can use values from 1 to 127. ( see the section above for more
information on identifiers and how they are used in replication. )

The identifier is setup on the builder as follows.

```java
TcpTransportAndNetworkConfig tcpConfig = ...
map = ChronicleMapBuilder
    .of(IntValue.class, CharSequence.class)
    .averageValue(averageValue)
    .entries(1_000_000)
    .replication(identifier, tcpConfig)
    .create();
```


#### Bootstrapping
When a node is connected over the network to an active grid of nodes. It must first receive any data
that it does not have from the other nodes. Eventually, all the nodes in the grid have to hold a
copy of exactly the same data. We refer to this initial data load phase as bootstrapping.
Bootstrapping by its very nature is point to point, so it is only performed over TCP replication.
For architectures that wish to use UDP replication it is advised you use TCP Replication as well. A
grid which only uses UDP replication will miss out on the bootstrapping, possibly leaving the nodes
in an inconsistent state. To avoid this, if you would rather reduce the amount of TCP traffic on
your network, we suggest you consider using a throttle TCP replication along with UDP replication.
Bootstrapping is not used when the nodes are on the same server, so for this case, TCP replication
is not required.

#### Identifier
Each map is allocated a unique identifier

Server 1 has:
```
.replication((byte) 1, tcpConfigServer1)
```

Server 2 has:
```
.replication((byte) 2, tcpConfigServer2)
```

Server 3 has:
```
.replication((byte) 3, tcpConfigServer3)
```

If you fail to allocate a unique identifier replication will not work correctly.

#### Port
Each map must be allocated a unique port, the port has to be unique per server, if the maps are
running on different hosts they could be allocated the same port, but in our example we allocated
them different ports, we allocated map1 port 8076 and map2 port 8077. Currently we don't support
data forwarding, so it important to connect every remote map, to every other remote map, in other
words you can't have a hub configuration where all the data passes through a single map which every
other map is connected to. So currently, if you had 4 servers each with a Chronicle Map, you would
require 6 connections.

In our case we are only using 2 maps, this is how we connected map1 to map 2.
```
TcpTransportAndNetworkConfig.of(8076, new InetSocketAddress("localhost", 8077))
                    .heartBeatInterval(1, SECONDS);
```
you could have put this instruction on map2 instead, like this 
```
TcpTransportAndNetworkConfig.of(8077, new InetSocketAddress("localhost", 8076))
                    .heartBeatInterval(1, SECONDS);
```
even though data flows from map1 to map2 and map2 to map1 it doesn't matter which way you connected
this, in other words its a bidirectional connection. 

#### Configuring Three Way TCP/IP Replication

![TCP/IP Replication 3Way](http://chronicle.software/wp-content/uploads/2014/09/Screen-Shot-2014-10-27-at-18.19.05.png)

Below is example how to set up tcpConfig for 3 host

```java
String hostServer1 = "localhost"; // change this to your host
int serverPort1 = 8076;           // change this to your port
InetSocketAddress inetSocketAddress1 = new InetSocketAddress(hostServer1, serverPort1);

String hostServer2 = "localhost"; // change this to your host
int  serverPort2= 8077;           // change this to your port
InetSocketAddress inetSocketAddress2 = new InetSocketAddress(hostServer2, serverPort2);

String hostServer3 = "localhost"; // change this to your host
int serverPort3 = 8078;           // change this to your port
InetSocketAddress inetSocketAddress3 = new InetSocketAddress(hostServer3, serverPort3);

// this is to go on server 1
TcpTransportAndNetworkConfig tcpConfigServer1 =
        TcpTransportAndNetworkConfig.of(serverPort1);

// this is to go on server 2
TcpTransportAndNetworkConfig tcpConfigServer2 = TcpTransportAndNetworkConfig
        .of(serverPort2, inetSocketAddress1);

// this is to go on server 3
TcpTransportAndNetworkConfig tcpConfigServer3 = TcpTransportAndNetworkConfig
        .of(serverPort3, inetSocketAddress1, inetSocketAddress2);
```     

#### Heart Beat Interval
We set a heartBeatInterval, in our example to 1 second
``` java
 heartBeatInterval(1, SECONDS)
```
A heartbeat will only be send if no data is transmitted, if the maps are constantly exchanging data
no heartbeat message is sent. If a map does not receive either data of a heartbeat the connection
is dropped and re-established.

### Multiple Chronicle Maps - Network Distributed

![Chronicle Maps Network Distributed](http://chronicle.software/wp-content/uploads/2014/07/Chronicle-Map_channels_diagram_02.jpg)

ChronicleMap TCP Replication lets you distribute a single ChronicleMap, to a number of servers
across your network. Replication is point to point and the data transfer is bidirectional, so in the
example of just two servers, they only have to be connected via a single TCP socket connection and
the data is transferred both ways. This is great, but what if you wanted to replicate more than
just one ChronicleMap, what if you were going to replicate two ChronicleMaps across your network,
unfortunately with just TCP replication you would have to have two tcp socket connections, which is
not ideal. This is why we created the `ReplicationHub`. The `ReplicationHub` lets you replicate numerous
ChronicleMaps via a single point to point socket connection.

The `ReplicationHub` encompasses TCP replication, where each map has to be given a
unique identifier, but when using the `ReplicationHub` we use a channel to identify the map,
rather than the identifier.  The identifier is used to identify the host/server which broadcasts the
update. Put simply:

* Each host must be given a unique identifier.
* Each map must be given a unique channel.

``` java
byte identifier= 2;
ReplicationHub replicationHub = ReplicationHub.builder()
                    .tcpTransportAndNetwork(tcpConfig)
                    .createWithId(identifier);
```

In this example above the `ReplicationHub` is given the identifier of 2.

With channels you are able to attach additional maps to a `ReplicationChannel` once its up and
running.

When creating the `ReplicationChannel` you should attach your tcp or udp configuration :
``` java
byte identifier = 1;
ReplicationHub replicationHub = ReplicationHub.builder()
                    .tcpTransportAndNetwork(tcpConfig)
                    .createWithId(identifier);
```

Attaching a `ReplicationChannel` to the map :

``` java
short channel = (short) 2;
ChronicleMap<IntValue.class, CharSequence> map = ChronicleMap
    .of(IntValue.class, CharSequence.class)
    .averageValue(averageValue)
    .entries(1000)
    .instance().replicatedViaChannel(replicationHub.createChannel(channel))
    .create();
```

The Chronicle channel is use to identify which map is to be replicated to which other map on
the remote node. In the example above this is assigned to '(short) 1', so for example if you have
two maps, lets call them map1 and map2, you could assign them with chronicle
channels 1 and 2 respectively. Map1 would have the chronicle channel of 1 on both servers. You
should not confuse the Chronicle Channels with the identifiers, the identifiers are unique per
replicating node ( in this case which host, the reason we say replicating node rather than host as it is
possible to have more than one replicating node per host if each of them had a different TCP/IP port ), where as the chronicle channels are used to identify which map you are referring. No additional socket
 connection is made per chronicle channel that
you use, so we allow up to 32767 chronicle channels.

If you inadvertently got the chronicle channels around the wrong way, then chronicle would attempt
to replicate the wrong maps data. The chronicle channels don't have to be in order but they must be
unique for each map you have.

#### Channels and ReplicationChannel - Example

``` java

import net.openhft.chronicle.hash.replication.ReplicationChannel;
import net.openhft.chronicle.hash.replication.ReplicationHub;
import net.openhft.chronicle.hash.replication.TcpTransportAndNetworkConfig;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

...

ChronicleMap<CharSequence, CharSequence> favoriteColourServer1, favoriteColourServer2;
ChronicleMap<CharSequence, CharSequence> favoriteComputerServer1, favoriteComputerServer2;


// server 1 with  identifier = 1
{
    ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
        .of(CharSequence.class, CharSequence.class)
        .averageKeySize(10).averageValueSize(10)
        .entries(1000);

    byte identifier = (byte) 1;

    TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
            .of(8086, new InetSocketAddress("localhost", 8087))
            .heartBeatInterval(1, TimeUnit.SECONDS);

    ReplicationHub hubOnServer1 = ReplicationHub.builder()
            .tcpTransportAndNetwork(tcpConfig)
            .createWithId(identifier);

    // this demotes favoriteColour
    short channel1 = (short) 1;

    ReplicationChannel channel = hubOnServer1.createChannel(channel1);
    favoriteColourServer1 = builder.instance()
            .replicatedViaChannel(channel).create();

    favoriteColourServer1.put("peter", "green");

    // this demotes favoriteComputer
    short channel2 = (short) 2;

    favoriteComputerServer1 = builder.instance()
            .replicatedViaChannel(hubOnServer1.createChannel(channel2)).create();

    favoriteComputerServer1.put("peter", "dell");
}

// server 2 with  identifier = 2
{
    ChronicleMapBuilder<CharSequence, CharSequence> builder = ChronicleMapBuilder
        .of(CharSequence.class, CharSequence.class)
        .averageKeySize(10).averageValueSize(10)
        .entries(1000);

    byte identifier = (byte) 2;

    TcpTransportAndNetworkConfig tcpConfig = TcpTransportAndNetworkConfig
            .of(8087).heartBeatInterval(1, TimeUnit.SECONDS);

    ReplicationHub hubOnServer2 = ReplicationHub.builder()
            .tcpTransportAndNetwork(tcpConfig)
            .createWithId(identifier);

    // this demotes favoriteColour
    short channel1 = (short) 1;

    favoriteColourServer2 = builder.instance()
            .replicatedViaChannel(hubOnServer2.createChannel(channel1)).create();

    favoriteColourServer2.put("rob", "blue");

    // this demotes favoriteComputer
    short channel2 = (short) 2;

    favoriteComputerServer2 = builder.instance()
            .replicatedViaChannel(hubOnServer2.createChannel(channel2)).create();

    favoriteComputerServer2.put("rob", "mac");
    favoriteComputerServer2.put("daniel", "mac");
}

// allow time for the recompilation to resolve
for (int t = 0; t < 2500; t++) {
    if (favoriteComputerServer2.equals(favoriteComputerServer1) &&
            favoriteColourServer2.equals(favoriteColourServer1))
        break;
    Thread.sleep(1);
}

assertEquals(favoriteComputerServer1, favoriteComputerServer2);
assertEquals(3, favoriteComputerServer2.size());

assertEquals(favoriteColourServer1, favoriteColourServer2);
assertEquals(2, favoriteColourServer1.size());

favoriteColourServer1.close();
favoriteComputerServer2.close();
favoriteColourServer2.close();
favoriteColourServer1.close();

```

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

#### Example - Simple logging

Just log all modification operations on `ChronicleMap`

```
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

```
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

```
enum DualLockSuccess {SUCCESS, FAIL}
```

```
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

```
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
```
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

#### Example - CRDT values for replicated Chronicle Maps - Grow-only set

`Set` values won't replace each other on replication, but will converge to a single, common set,
the union of all elements added to all sets on all replicated nodes.

```
class GrowOnlySetValuedMapEntryOperations<K, E>
        implements MapEntryOperations<K, Set<E>, Void> {

    private static final GrowOnlySetValuedMapEntryOperations INSTANCE =
            new GrowOnlySetValuedMapEntryOperations();

    public static <K, E>
    MapEntryOperations<K, Set<E>, Void> growOnlySetValuedMapEntryOperations() {
        return GrowOnlySetValuedMapEntryOperations.INSTANCE;
    }

    private GrowOnlySetValuedMapEntryOperations() {}

    @Override
    public Void remove(@NotNull MapEntry<K, Set<E>> entry) {
        throw new UnsupportedOperationException("Map with grow-only set values " +
                "doesn't support map value removals");
    }
}
```

```
class GrowOnlySetValuedMapRemoteOperations<K, E>
        implements MapRemoteOperations<K, Set<E>, Void> {

    private static final GrowOnlySetValuedMapRemoteOperations INSTANCE =
            new GrowOnlySetValuedMapRemoteOperations();

    public static <K, E>
    MapRemoteOperations<K, Set<E>, Void> growOnlySetValuedMapRemoteOperations() {
        return GrowOnlySetValuedMapRemoteOperations.INSTANCE;
    }

    private GrowOnlySetValuedMapRemoteOperations() {}

    @Override
    public void put(MapRemoteQueryContext<K, Set<E>, Void> q, Data<Set<E>, ?> newValue) {
        MapReplicableEntry<K, Set<E>> entry = q.entry();
        if (entry != null) {
            Set<E> merged = new HashSet<>(entry.value().get());
            merged.addAll(newValue.get());
            q.replaceValue(entry, q.wrapValueAsValue(merged));
        } else {
            q.insert(q.absentEntry(), newValue);
            q.entry().updateOrigin(q.remoteIdentifier(), q.remoteTimestamp());
        }
    }

    @Override
    public void remove(MapRemoteQueryContext<K, Set<E>, Void> q) {
        throw new UnsupportedOperationException();
    }
}
```

Usage:
```
HashSet<Integer> averageValue = new HashSet<>();
for (int i = 0; i < AVERAGE_SET_SIZE; i++) {
    averageValue.add(i);
}
ChronicleMap<Integer, Set<Integer>> map1 = ChronicleMapBuilder
        .of(Integer.class, (Class<Set<Integer>>) (Class) Set.class)
        .entries(100)
        .averageValue(averageValue)
        .entryOperations(growOnlySetValuedMapEntryOperations())
        .remoteOperations(growOnlySetValuedMapRemoteOperations())
        .replication((byte) 1, /* ... replicated nodes */)
        .instance()
        .name("map1")
        .create();
```
