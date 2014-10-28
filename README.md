*We can help you get Chronicle up and running in your organisation, we suggest you invite us in for
consultancy, charged on an ad-hoc basis, we can discuss the best options tailored to your individual
requirements. - [Contact Us](sales@higherfrequencytrading.com)*

*Or you may already be using Chronicle and just want some help - [find out more..](http://openhft.net/support/)*

# Chronicle Map

Replicate your Key Value Store across your network, with consistency, durability and performance.
![Chronicle Map](http://openhft.net/wp-content/uploads/2014/07/ChronicleMap_200px.png)


#### Maven Artifact Download
```xml
<dependency>                                   
  <groupId>net.openhft</groupId>
  <artifactId>chronicle-map</artifactId>
  <version><!--replace with the latest version--></version>
</dependency>
```
Click here to get the [Latest Version Number](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22net.openhft%22%20AND%20a%3A%22chronicle-map%22) 


#### Contents

* [Should I use Chronicle Queue or Chronicle Map](https://github.com/OpenHFT/Chronicle-Map#should-i-use-chronicle-queue-or-chronicle-map)
* [What is the difference between SharedHashMap and Chronicle Map](https://github.com/OpenHFT/Chronicle-Map#what-is-the-difference-between-sharedhashmap-and-chronicle-map)
* [Overview](https://github.com/OpenHFT/Chronicle-Map#overview)
* [JavaDoc](http://openhft.github.io/Chronicle-Map/apidocs)
* [Getting Started Guide](https://github.com/OpenHFT/Chronicle-Map#getting-started)
 *  [Simple Construction](https://github.com/OpenHFT/Chronicle-Map#simple-construction)
 *   [Maven Download](https://github.com/OpenHFT/Chronicle-Map#maven-artifact-download-1)
 *   [Snapshot Download](https://github.com/OpenHFT/Chronicle-Map#maven-snapshot-download)
 *   [Key Value Object Types](https://github.com/OpenHFT/Chronicle-Map#key-value-object-types)
 *   [Sharing Data Between Two or More Maps](https://github.com/OpenHFT/Chronicle-Map#sharing-data-between-two-or-more-maps)
 *   [Entries](https://github.com/OpenHFT/Chronicle-Map#entries)
 *   [Size of Space Reserved on Disk](https://github.com/OpenHFT/Chronicle-Map#size-of-space-reserved-on-disk)
 *   [Chronicle Map Interface](https://github.com/OpenHFT/Chronicle-Map#chronicle-map-interface)
* [Oversized Entries Support] (https://github.com/OpenHFT/Chronicle-Map/blob/master/README.md#oversized-entries-support)  
* [Serialization](https://github.com/OpenHFT/Chronicle-Map#serialization)
  *   [Simple Types](https://github.com/OpenHFT/Chronicle-Map#simple-types)
  *   [Complex Types](https://github.com/OpenHFT/Chronicle-Map#complex-types)
* [Close](https://github.com/OpenHFT/Chronicle-Map#close)
* [TCP / UDP Replication](https://github.com/OpenHFT/Chronicle-Map#tcp--udp-replication)
 * [TCP / UDP Background.](https://github.com/OpenHFT/Chronicle-Map#tcp--udp-background)
 *   [How to setup UDP Replication](https://github.com/OpenHFT/Chronicle-Map#how-to-setup-udp-replication)
 *  [TCP/IP Throttling](https://github.com/OpenHFT/Chronicle-Map#tcpip--throttling)
 *   [Replication How it works](https://github.com/OpenHFT/Chronicle-Map#replication-how-it-works)
 *  [Multiple Chronicle Maps on a the same server with Replication](https://github.com/OpenHFT/Chronicle-Map#multiple-chronicle-maps-on-a-the-same-server-with-replication)
 *   [Identifier for Replication](https://github.com/OpenHFT/Chronicle-Map#identifier-for-replication)
 *   [Bootstrapping](https://github.com/OpenHFT/Chronicle-Map#bootstrapping)
 *      [Identifier](https://github.com/OpenHFT/Chronicle-Map#identifier)
 * [Port](https://github.com/OpenHFT/Chronicle-Map#port)
 * [Heart Beat Interval](https://github.com/OpenHFT/Chronicle-Map#heart-beat-interval)
* [Channels and the Channel Provider](https://github.com/OpenHFT/Chronicle-Map#channels-and-channelprovider)
* [Stateless Client](https://github.com/OpenHFT/Chronicle-Map#stateless-client)

#### Miscellaneous

 * [Known Issues](https://github.com/OpenHFT/Chronicle-Map#known-issues)
 * [Stackoverflow](http://stackoverflow.com/tags/chronicle/info)
 * [Development Tasks - JIRA] (https://higherfrequencytrading.atlassian.net/browse/HCOLL)
 * [Use Case Which include Chronicle Map] (http://openhft.net/products/chronicle-engine/)

#### Examples

 * [Hello World - A map which stores data off heap](https://github.com/OpenHFT/Chronicle-Map/blob/master/README.md#example--simple-hello-world)
 * [Sharing the map between two ( or more ) processes on the same computer](https://github.com/OpenHFT/Chronicle-Map/blob/master/README.md#example--sharing-the-map-on-two--or-more--processes-on-the-same-machine)
 * [Replicating data between process on different servers with TCP/IP Replication](https://github.com/OpenHFT/Chronicle-Map/blob/master/README.md#example--replicating-data-between-process-on-different-servers-via-tcp)
 * [Replicating data between process on different servers with UDP] (https://github.com/OpenHFT/Chronicle-Map/blob/master/README.md#example--replicating-data-between-process-on-different-servers-using-udp)
 *  [Creating a Chronicle Set and adding data to it](https://github.com/OpenHFT/Chronicle-Map/blob/master/README.md#example--creating-a-chronicle-set-and-adding-data-to-it)

#### Performance Topics

* [Chronicle Map with Large Data ](https://github.com/OpenHFT/Chronicle-Map#chronicle-map-with-large-data)
* [Lock Contention] (https://github.com/OpenHFT/Chronicle-Map/blob/master/README.md#lock-contention)
* [Better to use small keys](https://github.com/OpenHFT/Chronicle-Map#better-to-use-small-keys)
* [ConcurrentHashMap v ChronicleMap](https://github.com/OpenHFT/Chronicle-Map#concurrenthashmap-v-chroniclemap)

### Overview
Chronicle Map implements the `java.util.concurrent.ConcurrentMap`, however unlike the standard
java map, ChronicleMap is able to share your entries accross processes:

![](http://openhft.net/wp-content/uploads/2014/07/Chronicle-Map-diagram_04.jpg)

## When to use
#### When to use HashMap
If you compare `HashMap`, `ConcurrentHashMap` and `ChronicleMap`, most of the maps in your system
are likely to be HashMap.  This is because `HashMap` is lightweight and synchronized HashMap works
well for lightly contended use cases.  By contention I mean, how many threads on average are trying
to use a Map.  One reason you can't have many contended resources, is that you only have so many
CPUs and they can only be accessing so many resources at once (ideally no more than one or two
per thread at a time).

####  When to use ConcurrentHashMap
`ConcurrentHashMap` scales very well when highly contended.  It uses more memory but if you only
have a few of them, this doesn't matter.  They have higher throughput than the other two solutions,
but also it creates the highest garbage.  If garbage pressure is an issue for you, you may want
to consider `ChronicleMap`

One of the main differences between chronicle and ConcurrentHashMap, is how you go about creating
an instance see the getting started guide below for details.

####  When to use Chronicle Map
If you have;
* lots of small key-values
* you want to minimise garbage produced, and medium lived objects.
* you need to share data between JVMs
* you need persistence

#### Should I use Chronicle Queue or Chronicle Map
Chronicle queue is designed to send every update. If your network can't do this something has
to give. You could compress the data but at some point you have to work within the limits of your
hardware or get more hardware. Chronicle Map on the other hand sends the latest value only.
This will naturally drop updates and is a more natural choice for low bandwidth connections.

#### What is the difference between [SharedHashMap](https://github.com/OpenHFT/HugeCollections) and Chronicle Map
SharedHashMap is an outdated version of ChronicleMap project.
Effectively SharedHashMap has just been renamed to ChronicleMap, to further enrich the Chronicle
product suite. In addition, The original Chronicle has been renamed to Chronicle Queue.


## Getting Started

#### Tutorial 1 - Creating an instance of Chronicle Map
[![ScreenShot](http://openhft.net/wp-content/uploads/2014/09/Screen-Shot-2014-10-14-at-17.49.36.png)](http://openhft.net/chronicle-map-video-tutorial-1/)

### Simple Construction

To download the JAR which contains Chronicle Map, we recommend you use maven, which will download it
from [Maven Central](http://search.maven.org), once you have installed maven, all you have to do is
add the following to your projects `pom.xml`:

#### Maven Artifact Download
```xml
<dependency>
  <groupId>net.openhft</groupId>
  <artifactId>chronicle-map</artifactId>
  <version><!--replace with the latest version--></version>
</dependency>
```
To get the latest version number
[Click Here](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22net.openhft%22%20AND%20a%3A%22chronicle-map%22) 

when you add ( the above dependency ) to your pom maven will usually attempt to download the release artifacts from: 
```
http://repo1.maven.org/maven2/net/openhft/chronicle-map
```

#### Maven Snapshot Download
If you want to try out the latest pre-release code, you can download the snapshot artifact manually from: 
```xml
https://oss.sonatype.org/content/repositories/snapshots/net/openhft/chronicle-map/
```
a better way is to add the following to your setting.xml, to allow maven to download snapshots :

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
  <version>1.0.1-SNAPSHOT</version>
</dependency>
```

#### Key Value Object Types

Unlike HashMap which will support any heap object, Chronicle Map only works with objects that it 
can store off heap, so the objects have to be  :  (one of the following )

- AutoBoxed primitives - for good performance.
- Strings - for good performance.
- implements Serializable  
- implements Externalizable ( with a public default constructor ) 
- implements our custom interface BytesMarshallable ( with a public default constructor ) - use 
this for best performance.

or value objects that are created through, a directClass interface, for example : 
``` java
      ChronicleMap<String, BondVOInterface> chm = OffHeapUpdatableChronicleMapBuilder
               .of(String.class, BondVOInterface.class)
               .create();

```

Object graphs can also be included as long as the outer object supports Serializable, Externalizable or BytesMarshallable.


#### Java Class Construction

Creating an instance of Chronicle Map is a little more complexed than just calling a constructor.
To create an instance you have to use the ChronicleMapBuilder.

``` java
import net.openhft.chronicle.map.*
.....

try {

    String tmp = System.getProperty("java.io.tmpdir");
    String pathname = tmp + "/shm-test/myfile.dat";

    File file = new File(pathname);

    ChronicleMapBuilder<Integer, CharSequence> builder =
        ChronicleMapBuilder.of(Integer.class, CharSequence.class);
    ConcurrentMap<Integer, CharSequence> map = builder.file(file).create();
 
} catch (IOException e) {
    e.printStackTrace();
}
```

Chronicle Map stores its data off the java heap, If you wish to share this off-heap memory between
processes on the same server, you must provide a "file", this file must be the same "file" for all
the instances of Chronicle Map on the same server. The name and location of the "file" is entirely
up to you.  For the best performance on many unix systems we recommend using
[tmpfs](http://en.wikipedia.org/wiki/Tmpfs).

If instead, you do not wish to replicate between processes on the same server or if you are only
using TCP replication to replicate between servers, you do not have to provide the "file",
so you can call the `create()` method on you ChronicleMapBuilder without providing the file 
parameter:
```
ConcurrentMap<Integer, CharSequence> map = builder.create();
```

### Sharing Data Between Two or More Maps
Since this file is memory mapped, if you were to create another instance of the Chronicle Map,
pointing to the same file, both Chronicle Maps use this file as a common memory store, which they
both read and write into. The good thing about this is the two ( or more instances of the Chronicle Map )
don't have to be running in the same java process. Ideally and for best performance, the two processes
should be running on the same server. Since the file is memory mapped, ( in most cases ) the read
and writes to the file are hitting the disk cache. Allowing the chronicle map to exchange data
between processes by just using memory and in around 40 nanoseconds. 

``` java 
ConcurrentMap<Integer, CharSequence> map1, map2;

// this could could be on one process
map1 = ChronicleMapBuilder.of(Integer.class, CharSequence.class).file(file).create();

// this could be on the other process
map2 = ChronicleMapBuilder.of(Integer.class, CharSequence.class).file(file).create();
```
Note: In order to share data between map1 and map2, the file has to point to the same file location
on your server.

### Entries

One of the differences with Chronicle Map against ConcurrentHashMap, is that it can't be resized,
unlike the ConcurrentHashMap, Chronicle Map is not limited to the available on heap memory.
Resizing is a very expensive operation for Hash Maps, as it can stall your application, so as such
we don't do it. When you are building a Chronicle Map you can set the maximum number of entries that
you are ever likely to support, its ok to over exaggerate this number. As the Chronicle Map is not
limited to your available memory, At worst you will end up having a very large file on disk.

You set the maximum number of entries by the builder:

``` java
ConcurrentMap<Integer, CharSequence> map =
    ChronicleMapBuilder.of(Integer.class, CharSequence.class)
    .entries(1000) // set the max number of entries here
    .create();
```
In this example above we have set 1000 entries.

We have optimised chronicle, So that you can have situations where you either don't use;

- all the entries you have allowed for.  This works best on Unix where the disk space and memory
used reflect the number of actual entries, not the number you allowed for.

- all the space you allow for each entry.  This helps if you have entries which are multiple cache
lines (128 bytes +), only the lines you touch sit in your CPU cache and if you have multiple pages
(8+ Kbytes) only the pages you touch use memory or disk.  The CPU cache usage matters as it can be
10000x smaller than main memory.

### Size of space reserved on disk

In linux, if you looked at the size of the 'file', it will report the used entry size so if you
have just added one entry, it will report the size of this entry, but Windows will report
the reserved size, as it reserves the disk space eagerly ( in fact windows also reserves the memory
eagerly as well ) in other words number-of-entries x entry-size. 

so on linux, if your type
``` 
# It shows you the extents. 
ls -l <file>

# It shows you how much is actually used.
du <file>
```

To illustrate this with an example - On Ubuntu we can create a 100 TB chronicle map.  Both `top` and
`ls -l` say the process virtual size / file size is 100 TB, however the resident memory via `du`
says the size is 71 MB after adding 10000 entries. You can see the size actually used with du.


### Chronicle Map Interface 
The Chronicle Map interface adds a few methods above an beyond the standard ConcurrentMap,
the ChronicleMapBuilder can also be used to return the ChronicleMap, see the example below :

``` java
ChronicleMap<Integer, CharSequence> map =
    ChronicleMapBuilder.of(Integer.class, CharSequence.class).create();
```
One way to achieve good performance is to focus on unnecessary object creation as this reduces
the amount of work that has to be carried out by the Garbage Collector. As such ChronicleMap
supports the following methods :

 - [`V getUsing(K key, V value);`](http://openhft.github.io/Chronicle-Map/apidocs/net/openhft/chronicle/map/ChronicleMap.html#getUsing-K-V-)
 - [`V acquireUsing(K key, V value);`](http://openhft.github.io/Chronicle-Map/apidocs/net/openhft/chronicle/map/ChronicleMap.html#acquireUsing-K-V-)

These methods let you provide the object which the data will be written to, even if the object it
immutable. For example 

``` java
StringBuilder myString = new StringBuilder(); 
StringBuilder myResult = map.getUsing("key", myString);
// at this point the myString and myResult will both point to the same object
```

The `map.getUsing()` method is similar to `get()`, but because Chronicle Map stores its data off
heap, if you were to call get("key"), a new object would be created each time, map.getUsing() works
by reusing the heap memory which was used by the original Object "myString". This technique provides
you with better control over your object creation.

Exactly like `map.getUsing()`, `acquireUsing()` will give you back a reference to an value based on
a key, but unlike `getUsing()` if there is not an entry in the map for this key the entry will be
added and the value return will we the same value which you provided.



#### Use getUsing(..) or acquireUsing(..) to avoid Object creation

The point of these methods is to avoid creating any objects.

Pattern 1
``` java
// get or create/initialise add needed
// assumption: I need this key value to exist or be initialised.
map.acquireUsing(key, value);

// use value

if (changed)
     map.put(key, value);

```

Pattern 2
``` java
// get or don't do anything else.
// assumption: I only need the value if it exists, otherwise I will do something else, or nothing.
if (map.getUsing(key, value) != null) {
     // use value

     if (changed)
         map.put(key, value);
} else {
     // don't use value
}
```
 

## Oversized Entries Support

It is possible for the size of your entry to be twice as large as the maximum entry size, 
we refer to this type of entry as an oversized entry. Oversized entries are there to cater for the case 
where only a small
percentage of your entries are twise as large as the others, in this case your large entry will
span across two entries. The alternative would be to increase your maximum entry size to be similar
to the size of the largest entry, but this approach is wasteful of memory, especially when most
entries are no where near the max entry size.  

## Serialization

Chronicle Map stores your data into off heap memory, so when you give it a Key or Value, it will
serialise these objects into bytes.

### Simple Types
If you are using simple auto boxed objects based on the primitive types, Chronicle Map will
automatically handle the serialisation for you.  

### Complex Types
For anything other than the standard object, the Objects either have to :
* implement "java.io.Serializable" ( which we don't recommend as this can be slow )
* we also support "java.io.Externalizable", we recommend this over Serializable as its usually faster.
* or for the best performance implement net.openhft.lang.io.serialization.BytesMarshallable,
an example of how to do this can be found at "IntValue$$Native"
* alternatively, you could write a "Custom Marshaller", the custom marshaller can be implemented
for a single type or a number of types.

### Close
Unlike ConcurrentHashMap, chronicle map stores its data off heap, often in a memory mapped file.
Its recommended that you call close() once you have finished working with a Chronicle Map.
``` java
map.close()
```

You only need to close to clean up resources deterministically.  If your program is exiting, 
you don't need to close the collection, as Chronicle never knows when the program might crash, 
so we have designed it so you don't have to close() it.

WARNING : If you call close too early before you have finished working with the map, this can cause
your JVM to crash. Close MUST BE the last thing that you do with the map.



# TCP / UDP Replication

Chronicle Hash Map supports both TCP and UDP replication

![TCP/IP Replication](http://openhft.net/wp-content/uploads/2014/07/Chronicle-Map-TCP-Replication_simple_02.jpg)


### TCP / UDP Background.
TCP/IP is a reliable protocol, what this means is unless you have a network failure or hardware
outage the data is guaranteed to arrive. TCP/IP provides point to point connectivity. So in effect
( over simplified ), if the message was sent to 100 hosts, The message would have to be sent
100 times. With UDP, the message is only sent once. This is ideal if you have a large number of
hosts and you wish to broadcast the same data to each off them.   However, one of the big drawbacks
with UDP is its not a reliable protocol. This means, if the UDP message is Broadcast onto
the network, The hosts are not guaranteed to receive it, so they can miss data. Some solutions
attempt to build resilience into UDP, but arguably, this is in effect reinventing TCP/IP.

### How to setup UDP Replication
In reality on a good quality wired LAN, when using UDP, you will rarely miss messages, this is
a risk that we suggest you don't take. We suggest that whenever you use UDP replication you use it
in conjunction with a throttled TCP replication, therefore if a host misses a message over UDP, they
will later pick it up via TCP/IP. 

###  TCP/IP  Throttling
We are careful not to swamp your network with too much TCP/IP traffic, We do this by providing
a throttled version of TCP replication. This works because Chronicle Map only broadcasts the latest
update of each entry. 

### Replication How it works

Chronicle Map provides multi master hash map replication, What this means, is that each remote
hash-map, mirrors its changes over to another remote hash map, neither hash map is considered
the master store of data, each hash map uses timestamps to reconcile changes.
We refer to in instance of a remote hash-map as a node.
A node can be connected to up to 128 other nodes.
The data that is stored locally in each node becomes eventually consistent. So changes made to one
node, for example by calling put() will be replicated over to the other node. To achieve a high
level of performance and throughput, the call to put() won’t block, 
With concurrentHashMap, It is typical to check the return code of some methods to obtain the old
value for example remove(). Due to the loose coupling and lock free nature of this multi master
implementation,  this return value is only the old value on the nodes local data store. In other
words the nodes are only concurrent locally. Its worth realising that another node performing
exactly the same operation may return a different value. However reconciliation will ensure the maps
themselves become eventually consistent.

### Reconciliation 
If two ( or more nodes ) receive a change to their maps for the same key but different values, say
by a user of the maps, calling the put(key,value). Then, initially each node will update its local
store and each local store will hold a different value, but the aim of multi master replication is
to provide eventual consistency across the nodes. So, with multi master when ever a node is changed
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
on a single thread. However there is an edge case, If two nodes update their map at the same time
with different values, we had to deterministically resolve which update wins, because of eventual
consistency both nodes should end up locally holding the same data. Although it is rare two remote
nodes could receive an update to their maps at exactly the same time for the same key, we had
to handle this edge case, its therefore important not to rely on timestamps alone to reconcile
the updates. Typically the update with the newest timestamp should win, but in this example both
timestamps are the same, and the decision made to one node should be identical to the decision made
to the other. This dilemma is resolved by using a node identifier, the node identifier is a unique
'byte' value that is assigned to each node, So when the time stamps are the same if the remoteNodes
identifier is smaller than the local nodes identifier, this update will be accepted otherwise it
will be ignored.

### Multiple Chronicle Maps on a the same server with Replication
If two or more Chronicle Maps are on the same server, they exchange data via shared memory rather
than TCP or UDP replication. So if a Chronicle Map which is not performing TCP Replication is
updated, this update can be picked up by another Chronicle Map, this other Chronicle Hash Map could
be a TCP replicated Chronicle Map, In this example the TCP replicated Chronicle Map would then push
the update to the remote nodes.

Likewise, If the TCP replicated Chronicle Map was to received an update from a remote node, then
this update would be immediately available to all the Chronicle Maps on the server.

### Identifier for Replication
If all you are doing is replicating your chronicle maps on the same server you don't have to set up
TCP and UDP replication. You also don't have to set the identifiers. 

If however you wish to replicate data between 2 or more servers, then ALL of the Chronicle Maps
including those not actively participating in TCP or UDP replication must have the identifier set.
The identifier must be unique to each server. Each Chronicle Map on the same server must have
the same identifier. The reason that all Chronicle Maps must have the identifier set, is because
the memory is laid out slightly differently when using replication, so even if a Map is not actively
performing TCP or UDP replication its self, if it wishes to replicate with one that is, it must have
its memory laid out the same way to be compatible. 

If the identifiers are not set up uniquely then the updates will be ignored, as for example
a Chronicle Map set up with the identifiers equals '1', will ignore all events which contain
the remote identifier of '1', in other words Chronicle Map replication is set up to ignore updates
which have originated from itself. This is to avoid the circularity of events.

When setting up the identifier you can use values from 1 to 127. ( see the section above for more
information on identifiers and how they are used in replication. )

The identifier is setup on the builder as follows.

```java
TcpReplicationConfig tcpConfig = ...
map = ChronicleMapBuilder
    .of(Integer.class, CharSequence.class)
    .replicators(identifier, tcpReplicationConfig)
    .create();
```


   
### Bootstrapping 
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

### Identifier
Each map is allocated a unique identifier

Server 1 has:
```
.replicators((byte) 1, tcpReplicationConfigServer1)
```

Server 2 has:
```
.replicators((byte) 2, tcpReplicationConfigServer2)
```

Server 3 has:
```
.replicators((byte) 3, tcpReplicationConfigServer3)
```

If you fail to allocate a unique identifier replication will not work correctly.

### Port
Each map must be allocated a unique port, the port has to be unique per server, if the maps are
running on different hosts they could be allocated the same port, but in our example we allocated
them different ports, we allocated map1 port 8076 and map2 port 8077. Currently we don't support
data forwarding, so it important to connect every remote map, to every other remote map, in other
words you can't have a hub configuration where all the data passes through a single map which every
other map is connected to. So currently, if you had 4 servers each with a Chronicle Map, you would
require 6 connections.

In our case we are only using 2 maps, this is how we connected map1 to map 2.
```
TcpReplicationConfig.of(8076, new InetSocketAddress("localhost", 8077))
                    .heartBeatInterval(1, SECONDS);
```
you could have put this instruction on map2 instead, like this 
```
TcpReplicationConfig.of(8077, new InetSocketAddress("localhost", 8076))
                    .heartBeatInterval(1, SECONDS);
```
even though data flows from map1 to map2 and map2 to map1 it doesn't matter which way you connected
this, in other words its a bidirectional connection. 

### Configuring Three Way TCP/IP Replication

![TCP/IP Replication 3Way](http://openhft.net/wp-content/uploads/2014/09/Screen-Shot-2014-10-27-at-18.19.05.png)


Below is example how to set up tcpReplicationConfig for 3 host

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
TcpReplicationConfig tcpReplicationConfigServer1 = TcpReplicationConfig.of(serverPort1);

// this is to go on server 2
TcpReplicationConfig tcpReplicationConfigServer2 = TcpReplicationConfig.of(serverPort2,
        inetSocketAddress1);

// this is to go on server 3
TcpReplicationConfig tcpReplicationConfigServer3 = TcpReplicationConfig.of(serverPort3,
        inetSocketAddress1,inetSocketAddress2);        
```     

### Heart Beat Interval
We set a heartBeatInterval, in our example to 1 second
``` java
 heartBeatInterval(1, SECONDS)
```
A heartbeat will only be send if no data is transmitted, if the maps are constantly exchanging data
no heartbeat message is sent. If a map does not receive either data of a heartbeat the connection
is dropped and re-established.

# Channels and ChannelProvider

Chronicle Map TCP Replication lets you distribute a single Chronicle Map, to a number of servers
across your network. Replication is point to point and the data transfer is bidirectional, so in the
example of just two servers, they only have to be connected via a single tcp socket connection and
the data is transferred both ways. Which is great, however what if you wanted to replicate more than
just one chronicle map, what if you were going to replicate two chronicle maps across your network,
unfortunately with just TCP replication you would have to have two tcp socket connections, which is
not ideal. This is why we created Chronicle Channels. Channels let you replicate numerous
Chronicle Maps via a single point to point socket connection.

Chronicle Channels are similar to TCP replication, where each map has to be given a unique identifier, but
when using Chronicle Channels its the channels that are used to identify the map. 
The identifier is used to identify the host/server. Each host must be given a unique identifier. 
Each map must be given a unique Channel.


``` java
int maxEntrySize = 1024;
byte identifier= 2;
ChannelProvider channelProvider = new ChannelProviderBuilder()
                    .maxEntrySize(maxEntrySize)
                    .replicators(identifier, tcpReplicationConfig).create();
```

In this example above the channel is given the identifier of 2

In addition to specifying the identifier we also have to set the maximum entry size, this sets
the size of the memory buffers within the ChannelProvider.  This has to be set manually, with channels you
are able to attach additional maps to a ChannelProvider once its up and running, so the maximum size of each
entry in the map can not be known in advance and we don’t currently support automatic resizing
of buffers.

When creating the ChannelProvider you should attach your tcp or udp configuration 
``` java
int maxEntrySize = 1024;
byte identifier = 1;
ChannelProvider channelProvider = new ChannelProviderBuilder()
                    .maxEntrySize(maxEntrySize)
                    .replicators(identifier, tcpReplicationConfig).create();
```

Attaching ChannelProvider replication to the map:

``` java
short channel = (short) 2;
ChronicleMap<Integer, CharSequence> map = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
  .entries(1000)  
  .channel(channelProvider.createChannel(channel)
  .create();
```

The chronicle channel is use to identify which map is to be replicated to which other map on
the remote node, in the example above this is assigned to '(short) 1', so for example if you have
two maps, lets call them map1 and map2, you could assign them with chronicle
channels 1 and 2 respectively. Map1 would have the chronicle channel of 1 on both servers. You
should not confuse the Chronicle Channels with the identifiers, the identifiers are unique per
replicating node the chronicle channels are used to identify which map you are referring. No
additional socket connection is made per chronicle channel that you use, so we allow up to 32767
chronicle channels.

If you inadvertently got the chronicle channels around the wrong way, then chronicle would attempt
to replicate the wrong maps data. The chronicle channels don't have to be in order but they must be
unique for each map you have.


### Channels and ChannelProvider - Example

``` java
    // server 1 with identifier = 1
    {
        byte identifier = (byte) 1;

        TcpReplicationConfig tcpConfig = TcpReplicationConfig
                .of(8086, new InetSocketAddress("localhost", 8087))
                .heartBeatInterval(1, java.util.concurrent.TimeUnit.SECONDS);


        channelProviderServer1 = new ChannelProviderBuilder()
                .replicators(identifier, tcpConfig)
                .create();

        // this demotes favoriteColour
        short channel1 = (short) 1;

        favoriteColourServer1 = ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                .entries(1000)
                .channel(channelProviderServer1.createChannel(channel1)).create();


        // this demotes favoriteComputer
        short channel2 = (short) 2;

        favoriteComputerServer1 = ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                .entries(1000)
                .channel(channelProviderServer1.createChannel(channel2)).create();

      
        favoriteColourServer1.put("peter", "green");

        favoriteComputerServer1.put("peter", "dell");

    }

    // server 2 with identifier = 2
    {

        byte identifier = (byte) 2;

        TcpReplicationConfig tcpConfig =
                TcpReplicationConfig.of(8087)
                .heartBeatInterval(1, java.util.concurrent.TimeUnit.SECONDS);

        channelProviderServer2 = new ChannelProviderBuilder()
                .replicators(identifier, tcpConfig)
                .create();


        // this demotes favoriteColour
        short channel1 = (short) 1;

        favoriteColourServer2 = ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                .entries(1000)
                .channel(channelProviderServer2.createChannel(channel1)).create();

        // this demotes favoriteComputer
        short channel2 = (short) 2;

        favoriteComputerServer2 = ChronicleMapBuilder.of(CharSequence.class, CharSequence.class)
                .entries(1000)
                .channel(channelProviderServer2.createChannel(channel2)).create();


        favoriteColourServer2.put("rob", "blue");

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
    Assert.assertEquals(3, favoriteComputerServer2.size());


    assertEquals(favoriteColourServer1, favoriteColourServer2);
    Assert.assertEquals(2, favoriteColourServer1.size());
    
    favoriteComputerServer1.close();
    favoriteComputerServer2.close();
    favoriteColourServer1.close();
    favoriteColourServer2.close();
}
``` 

# Stateless Client

![](http://openhft.net/wp-content/uploads/2014/07/Chronicle-Map-remote-stateless-map_04_vB.jpg)

A stateless client is an instance of a `ChronicleMap` or a `ChronicleSet` that does not hold any 
data
 locally, all the Map or Set operations are delegated via a Remote Procedure Calls ( RPC ) to 
 another `ChronicleMap` or  `ChronicleSet`  which we will refer to as the server. The server will
 hold all your data, the server can not it’s self be a stateless client. Your stateless client must
 be connected to the server via TCP/IP. The stateless client will delegate all your method calls to
 the remote server. The stateless client operations will block, in other words the stateless client
 will wait for the server to send a response before continuing to the next operation. The stateless
 client could be  consider to be a ClientProxy to `ChronicleMap` or  `ChronicleSet`  running
 on another host.
 
 Below is an example of how to configure a stateless client.

``` java
final ChronicleMap<Integer, CharSequence> serverMap;
final ChronicleMap<Integer, CharSequence> statelessMap;

// server
{

    ChronicleMapBuilder.of(Integer.class, CharSequence.class)
            .replicators((byte) 2, TcpReplicationConfig.of(8076))
            .create();            
                       
    serverMap.put(10, "EXAMPLE-10");
}

// stateless client
{
    statelessMap = ChronicleMapBuilder.of(Integer.class, CharSequence.class)
            .stateless(StatelessBuilder.remoteAddress(new InetSocketAddress("localhost", 8076)))
            .create();

    Assert.assertEquals("EXAMPLE-10", statelessMap.get(10));
    Assert.assertEquals(1, statelessMap.size());
}

serverMap.close();
statelessMap.close();
```

When used with a stateless client, Each state-full server has to be configured with TCP 
replication, when you set up TCP Replication you must define a port for the replication to 
run on, the port you choose is up to you, but you should pick a free port that is not currently 
being used by another application. In this example we choose the port 8076


``` java
.replicators((byte) 2, TcpReplicationConfig.of(8076))  // sets the server to run on localhost:8076
``` 
  
On the "stateless client" we connect to the server via TCP/IP on localhost:8076 : 

``` java
.stateless(remoteAddress(new InetSocketAddress("localhost", 8076)))
```

but in your example you should choose the host of the state-full server and the port you allocated
 it. 

``` java
.stateless(StatelessBuilder.remoteAddress(new InetSocketAddress(<host of state-full server>, 
<port of state-full server>)))
```

the ".stateless(..)" method tells `ChronicleMap` that its going to build a stateless client. If you 
don’t add this line a normal state-full `ChronicleMap` will be created. For this example we ran both 
the client an the server on the same host ( hence the “localhost" setting ), 
but in a real life example the stateless client will typically be on a different server than the
state-full host. If you are aiming to create a stateless client and server on the same host, its
better not to do this, as the stateless client connects to the server via TCP/IP, 
you would get better performance if you connect to the server via heap memory, to read more about
sharing a map with heap memory
click [here](https://github.com/OpenHFT/Chronicle-Map#sharing-data-between-two-or-more-maps ) 

##### Close

its always important to close ChronicleMap's and `ChronicleSet` 's when you have finished with them
``` java
serverMap.close();
statelessMap.close();
``` 


#  Known Issues

##### Memory issue on Windows

Chronicle map lets you assign a map larger than your available memory, If you were to create more
entries than the available memory, chronicle map will page the segments that are accessed least to
disk, and load the recently used segments into available memory. This feature lets you work with
extremely large maps, it works brilliantly on Linux but unfortunately, this paging feature is not
supported on Windows, if you use more memory than is physically available on windows you will
experience the following error :

```java
Java frames: (J=compiled Java code, j=interpreted, Vv=VM code)
j sun.misc.Unsafe.compareAndSwapLong(Ljava/lang/Object;JJJ)Z+0
j net.openhft.lang.io.NativeBytes.compareAndSwapLong(JJJ)Z+13
j net.openhft.lang.io.AbstractBytes.tryLockNanos8a(JJ)Z+12
j net.openhft.lang.io.AbstractBytes.tryLockNanosLong(JJ)Z+41
j net.openhft.collections.AbstractVanillaSharedHashMap$Segment.lock()V+12
```

##### When Chronicle Map is Full

It will throw this exception :

```java
Caught: java.lang.IllegalStateException: VanillaShortShortMultiMap is full
java.lang.IllegalStateException: VanillaShortShortMultiMap is full
	at net.openhft.collections.VanillaShortShortMultiMap.nextPos(VanillaShortShortMultiMap.java:226)
	at net.openhft.collections.AbstractVanillaSharedHashMap$Segment.put(VanillaSharedHashMap.java:834)
	at net.openhft.collections.AbstractVanillaSharedHashMap.put0(VanillaSharedHashMap.java:348)
	at net.openhft.collections.AbstractVanillaSharedHashMap.put(VanillaSharedHashMap.java:330)
```

Chronicle Map doesn't resize automatically.  It is assumed you will make the virtual size of the map
larger than you need and it will handle this reasonably efficiently. With the default settings you
will run out of space between 1 and 2 million entries.

You should set the .entries(..) and .entrySize(..) to those you require.

##### Don't forget to set the EntrySize

If you put() and entry that is much larger than the max entry size set via entrySize(), 
the code will error. To see how to set the entry size the example below sets the entry size to 10, 
you should pick a size that is the size in bytes of your entries : 

```java
ChronicleMap<Integer, String> map =
             ChronicleMapBuilder.of(Integer.class, String.class)
                     .entrySize(10).create();
 
```

This example will throw an java.lang.IllegalArgumentException because the entrySize is too small.

```java
@Test
public void test() throws IOException, InterruptedException {
    ChronicleMap<Integer, String> map =
            ChronicleMapBuilder.of(Integer.class, String.class)
                    .entrySize(10).create();

    String value =   new String(new char[2000]);
    map.put(1, value);

    Assert.assertEquals(value, map.get(1));
}

```

If the entry size is dramatically too small ( like in the example below ), 
you will get a *malloc_error_break* :

```java
@Test
public void test() throws IOException, InterruptedException {
    ChronicleMap<Integer, String> map =
            ChronicleMapBuilder.of(Integer.class, String.class)
                    .entrySize(10).create();

    String value =   new String(new char[20000000]);
    map.put(1, value);

    Assert.assertEquals(value, map.get(1));
}
```

# Example : Simple Hello World

This simple chronicle map, works just like ConcurrentHashMap but stores its data off-heap. If you
want to use Chronicle Map to share data between java process you should look at the next exampl 

``` java 
Map<Integer, CharSequence> map = ChronicleMapBuilder.of(Integer.class,
        CharSequence.class).create();

map.put(1, "hello world");
System.out.println(map.get(1));

``` 

# Example : Sharing the map on two ( or more ) processes on the same machine

Lets assume that we had two server, lets call them server1 and server2, if we wished to share a map
between them, this is how we could set it up

``` java 

// --- RUN ON ONE JAVA PROCESS ( BUT ON THE SAME SERVER )
{
    File file = new File("a-new-file-on-your-sever");	
    Map<Integer, CharSequence> map1 = ChronicleMapBuilder.of(Integer.class,
            CharSequence.class).create(file); // this has to be the same file as used by map 2
    map1.put(1, "hello world");
}

// --- RUN ON THE OTHER JAVA PROCESS ( BUT ON THE SAME SERVER )
{
    File file = new File("a-new-file-on-your-sever");  // this has to be the same file as used by map 1
    Map<Integer, CharSequence> map1 = ChronicleMapBuilder.of(Integer.class,
            CharSequence.class).create(file);

    System.out.println(map1.get(1));
}
```


# Example : Replicating data between process on different servers via TCP/IP

Lets assume that we had two server, lets call them server1 and server2, if we wished to share a map
between them, this is how we could set it up

``` java 
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class YourClass {


    @Test
    public void test() throws IOException, InterruptedException {

        Map map1;
        Map map2;

//  ----------  SERVER1 1 ----------
        {

            // we connect the maps via a TCP/IP socket connection on port 8077

            TcpReplicationConfig tcpConfig = TcpReplicationConfig.of(8076, new InetSocketAddress("localhost", 8077))
                    .heartBeatInterval(1L, TimeUnit.SECONDS);
            ChronicleMapBuilder<Integer, CharSequence> map1Builder =
                    ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                            .entries(20000L)
                            .replicators((byte) 1, tcpConfig);


            map1 = map1Builder.create();
        }
//  ----------  SERVER2 on the same server as ----------

        {
            TcpReplicationConfig tcpConfig = TcpReplicationConfig.of(8077)
                    .heartBeatInterval(1L, TimeUnit.SECONDS);
            ChronicleMapBuilder<Integer, CharSequence> map2Builder =
                    ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                            .entries(20000L)
                            .replicators((byte) 2, tcpConfig);
            map2 = map2Builder.create();

            // we will stores some data into one map here
            map2.put(5, "EXAMPLE");
        }

//  ----------  CHECK ----------

// we are now going to check that the two maps contain the same data

// allow time for the recompilation to resolve
        int t = 0;
        for (; t < 5000; t++) {
            if (map1.equals(map2))
                break;
            Thread.sleep(1);
        }

        Assert.assertEquals(map1, map2);
        Assert.assertTrue(!map1.isEmpty());
    }

}
```

# Example : Replicating data between process on different servers using UDP

This example is the same as the one above, but it uses a slow throttled TCP/IP connection to fill in
updates that may have been missed when sent over UDP. Usually on a good network, for example a wired
LAN, UDP won’t miss updates. But UDP does not support guaranteed delivery, we recommend also running
a TCP connection along side to ensure the data becomes eventually consistent.  Note : It is possible
to use Chronicle without the TCP replication and just use UDP (  that’s if you like living dangerously ! )



``` java 
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class YourClass {


    @Test
    public void test() throws IOException, InterruptedException {

        Map map1;
        Map map2;

        int udpPort = 1234;

//  ----------  SERVER1 1 ----------
        {

            // we connect the maps via a TCP socket connection on port 8077

            TcpReplicationConfig tcpConfig = TcpReplicationConfig.of(8076, new InetSocketAddress("localhost", 8077))
                    .heartBeatInterval(1L, TimeUnit.SECONDS)

                            // a maximum of 1024 bits per millisecond
                    .throttlingConfig(ThrottlingConfig.throttle(1024, TimeUnit.MILLISECONDS));


            UdpReplicationConfig udpConfig = UdpReplicationConfig
                    .simple(Inet4Address.getByName("255.255.255.255"), udpPort);

            ChronicleMapBuilder<Integer, CharSequence> map1Builder =
                    ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                            .entries(20000L)
                            .replicators((byte) 1, tcpConfig, udpConfig);


            map1 = map1Builder.create();
        }
//  ----------  SERVER2 2 on the same server as ----------

        {
            TcpReplicationConfig tcpConfig = TcpReplicationConfig.of(8077)
                    .heartBeatInterval(1L, TimeUnit.SECONDS)
                    .throttlingConfig(ThrottlingConfig.throttle(1024, TimeUnit.MILLISECONDS));

            UdpReplicationConfig udpConfig = UdpReplicationConfig
                    .simple(Inet4Address.getByName("255.255.255.255"), udpPort);

            ChronicleMapBuilder<Integer, CharSequence> map2Builder =
                    ChronicleMapBuilder.of(Integer.class, CharSequence.class)
                            .entries(20000L)
                            .replicators((byte) 2, tcpConfig, udpConfig);

            map2 = map2Builder.create();

            // we will stores some data into one map here
            map2.put(5, "EXAMPLE");
        }

//  ----------  CHECK ----------

// we are now going to check that the two maps contain the same data

// allow time for the recompilation to resolve
        int t = 0;
        for (; t < 5000; t++) {
            if (map1.equals(map2))
                break;
            Thread.sleep(1);
        }

        Assert.assertEquals(map1, map2);
        Assert.assertTrue(!map1.isEmpty());

    }
}
```

# Example : Creating a Chronicle Set and adding data to it

This project also provides the Chronicle Set, `ChronicleSet` is built on Chronicle Map, so the builder
configuration are almost identical to `ChronicleMap` ( see above ), this example shows how to create
a simple off heap set
``` java 
        Set<Integer> set = ChronicleSetBuilder.of(Integer.class).create();
        
        set.add(1);
        set.remove(1)
```
and just like map it support shared memory and TCP replication.         
        
        
# Performance Topics


There are general principles we can give direction on - for specific advise we believe consulting
 to be the most productive solution.

We want the Map to be reasonably general purpose, so in broad terms we can say
- the key and values have to be self contained, ideally trees, rather than graphs.
- ideally values are similar lengths, however we support varying lengths.
- ideally you want to use primitives and use object recycling for performance, though this is not a requirement.
- ideally you have some idea as to the maximum number of entries, though it is not too important if
the maximum entries is above what you need.
- if for example you are working with, market depth, this  can be supported via an array of nested 
types.
- we support code generation of efficient custom serializes - See the examples where you provide 
an interface as the data type, the map will generate the implementation.



### Tuning Chronicle Map with Large Data 

Generally speaking `ChronicleMap` is slower then ConcurrentHashMap for a small number of entries, but
for a large number of entries ConcurrentHashMap doesn't scale as well as Chronicle Map, especially
when you start running low on heap. ConcurrentHashMap quickly becomes unusable whereas Chronicle Map
can still work when it is 20 times the size of a ConcurrentHashMap with an Out of Memory Error.
  
For example with a heap of 3/4 of say 32 GB main memory, you might get say 100 million entries but
when using most of the heap you might see 20-40 second gc pauses with `ChronicleMap` you could have
1000 million entries and see < 100 ms pauses (depending on your disk subsystem and how fast you
write your data)

Chronicle Map makes heavy use of the OS to perform the memory management and writing to disk. How it
behaves is very dependant on how you tune the kernel and what hardware you are using. You may get
bad behaviour when the kernel forces a very large amount of data to disk after letting a lot of
uncommited data build up. In the worst case scenario the OS will stop the process for tens of
seconds at a time ( even up to 40 seconds) rather than let the program continue. However, to get
into that state you have to be loading a lot of data which exceeds main memory with very little rest
(e.g. cpu processing). There are good use cases for bulk data loads, but you have to be careful how
this is done if you also want good worst case latency characteristics. (the throughput should be
much the same)

When you create a Chronicle Map, it has many segments. By default it has a minimum of 128, but one
for every 32 K entries. e.g. for 500M entries you can expect ~16K segments (being the next power of
2). With so many segments, the chances of a perfect hash distribution is low and so the Chronicle
Map allows for double what you asked for but is designed to do this with almost no extra main memory
(only extra virtual memory). This means when you ask for 500M * 256 bytes entries you actually get 1
BN possible entries (assuming a perfect hash distribution between segments) There is a small
overhead per entry of 16 - 24 bytes adding another 20 GB.

So while the virtual memory is 270 GB, it is expected that for 500 M entries you will be trying to
use no more than 20 GB (overhead/hash tables) + ~120 GB (entries)

When `ChronicleMap` has exhausted all the memory on your server, its not going to be so fast, for a
random access pattern you are entirely dependant on how fast your underlying disk is. If your home
directory is an HDD and its performance is around 125 IOPS (I/Os per second). Each lookup takes two
memory accesses so you might get around 65 lookups per second. For 100-200K operations you can
expect around 1600 seconds or 25-50 minutes. If you use an SSD, it can get around 230 K IOPS, or
about 115 K `ChronicleMap` lookups per second.


### Lock contention

If you see the following warning :

``` java 
WARNING:net.openhft.lang.io.AbstractBytes tryLockNanosLong0
WARNING: Thread-2, to obtain a lock took 0.129 seconds
``` 
 
It's likely you have lock contention, this can be due to : 

- a low number of segments and
- the machine was heavily over utilised, possibly with the working data set larger than main memory.
- you have a large number of threads, greater than the number of cores you have, doing nothing but hit one map.

It’s not possible to fully disable locking,  locking is done a a segment basis.
So, If you set a large number of actual segments, this will reduce your lock contention. 

See the example below to see how to set the number of segments :

``` java 
ChronicleMap<Long, String> map = ChronicleMapBuilder.of(Long.class, String.class)
   .entries(100)
   .actualSegments(100)    // set your number of segments here
   .create();
```  
 
Reducing lock contention will make this warning message go away, but this message maybe more of a symptom 
of a general problem with what the system is doing, so you may experience a delay anyway.

### Better to use small keys

If you put() a small number of large entries into Chronicle Map, you are unlikely to see any
performance gains over a standard map, So we recommend you use a standard ConcurrentHashMap, unless
you need Chronicle Maps other features.

Chronicle Map gives better performance for smaller keys and values due to the low overhead per
entry. It can use 1/5th the memory of ConcurrentHashMap. When you have larger entries, the overhead
per entry doesn't matter so much and the relative waste per entry starts to matter. For Example,
Chronicle Map assumes every entry is the same size and if you have 10kB-20kB entries the 10K entries
can be using 20 kB of virtual memory or at least 12 KB of actual memory (since virtual memory turns
into physical memory in multiples of a page)

As the `ChronicleMap` gets larger the most important factor is the use of CPU cache rather than main
memory, performance is constrained by the number of cache lines you have to touch to update/read an
entry. For large entries this is much the same as ConcurrentHashMap.  In this case, `ChronicleMap` is
not worse than ConcurrentHashMap but not much better.

For large key/values it is not total memory use but other factors which matter such as;
- how compact each entry is. Less memory used makes better use of the L3 cache and memory bus which
  is often a bottleneck in highly concurrent applications. 
- reduce the impact on GCs. The time to perform  GC and its impact is linear. Moving the bulk of
  your data off heap can dramatically improve throughput not to mention worst case latency.
- Large data structures take a long time to reload and having a persisted store significantly
  reduces restart times.
- data can be shared between processes. This gives you more design options to share between JVMS and
  support short lived tasks without having to use TCP.
- data can be replicated across machines.





### ConcurrentHashMap v ChronicleMap
ConcurrentHashMap ( CHM ) outperforms `ChronicleMap` ( CM ) on throughput.  If you don't need
the extra features SharedHashMap gives you, it is not worth the extra complexity it brings.
i.e. don't use it just because you think it is cool. The test can be found in
[ChronicleMapTest](https://github.com/OpenHFT/Chronicle-Map/blob/master/src/test/java/net/openhft/chronicle/map/ChronicleMapTest.java)
under testAcquirePerf() and testCHMAcquirePerf()

Chronicle Map out performs ConcurrentHashMap on memory consumption, and worst case latencies.
It can be used to reduce or eliminate GCs.

#### Performance Test for many small key-values
The following performance test consists of string keys of the form "u:0123456789" and an int
counter.  The update increments the counter once in each thread, creating an new entry if required.


| Number of entries | Chronicle* Throughput  |  Chronicle RSS  | HashMap* Throughput | HashMap Worst GC pause | HashMap RSS |
|------------------:|---------------:|---------:|---------------:|-------------------:|--------:|
|        10 million |      30 Mupd/s |     ½ GB |     155 Mupd/s |           2.5 secs |    9 GB |
|        50 million |      31 Mupd/s |    3⅓ GB |     120 Mupd/s |           6.9 secs |   28 GB |
|       250 million |      30 Mupd/s |    14 GB |     114 Mupd/s |          17.3 secs |   76 GB |
|      1000 million |      24 Mupd/s |    57 GB |           OOME |            43 secs |      NA |
|      2500 million |      23 Mupd/s |   126 GB |   Did not test |                 NA |      NA |

_*HashMap refers to ConcurrentHashMap, Chronicle refers to Chronicle Map_

Notes:
* `ChronicleMap` was tested with a 32 MB heap, CHM was test with a 100 GB heap.
* The `ChronicleMap` test had a small minor GC on startup of 0.5 ms, but not during the test.
  This is being investigated.
* `ChronicleMap` was tested "writing" to a tmpfs file system.

#### How does it perform when persisted?

Chronicle Map also supports persistence. In this regard there is no similar class in the JDK.

| Number of entries | Chronicle Throughput  |  Chronicle RSS |
|------------------:|---------------:|---------:|
|        10 million |      28 Mupd/s |     ½ GB |
|        50 million |      28 Mupd/s |     9 GB |
|       250 million |      26 Mupd/s |    24 GB |
|      1000 million |     1.3 Mupd/s |    85 GB |

Notes:
* Persistence was performed at a PCI-SSD which supports up to 230K IOPS and 900 MB/s write speed.
  This test didn't test the card to it's limit until the last test.
* The kernel tuning parameters for write back are important here.
  This explains the suddern drop off and this is being investigated.

The sysctl parameters used were approximately 10x the defaults to allow as many operations
to be performed in memory as possible.

    vm.dirty_background_ratio = 50
    vm.dirty_expire_centisecs = 30000
    vm.dirty_ratio = 90
    vm.dirty_writeback_centisecs = 5000
