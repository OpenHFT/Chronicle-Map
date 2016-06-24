# Chronicle Map 3 Compatibility

## The Java Implementation Compatibility

### Public API

For Chronicle Map, *public API*  is comprised of public classes that are included in [Javadoc](
http://www.javadoc.io/doc/net.openhft/chronicle-map/). Other public classes are not subject for
the following rules, they might be changed in any way, or removed.

 - Class and method names, types and number of parameters in already existing methods shouldn't be
 changed.
 - Any methods could be added to the classes and interfaces that are not subject for
 subclassing or implementing by the library users, such as `ChronicleMap`, `ChronicleMapBuilder`,
 `MapEntry`, but in a way that any existing code calling methods on those classes still compile, not
 break due to ambiguity in method resolution.
 - Only non-abstract (concrete or default) methods could be added to the classes and interfaces that
 are subject for subclassing by the library users, such as `MapMethods`, `MapEntryOperations`,
 `SizedReader`.

### Behaviour

Semantics and actual effects of API calls might change between Chronicle Map versions with
[different "minor" number](versioning.md), e. g. default serialization forms of key or value objects
of some types could change, "high level" configurations in `ChronicleMapBuilder` could produce
Chronicle Maps with different "low level" configurations, etc. Such changes should be documented in
release notes.

### Persistence File Binary Form

A persisted Chronicle Map store (a binary file), produced by a production release of the library,
should be accessible and updatable by any subsequent production release of the library in 3.x
branch, unless the older version has bugs that lead to data corruption.

To support this,
 - Serialization forms of any classes shouldn't be changed. When e. g. a marshaller class (say,
 `StringBytesReader`) of objects of some type is changed, and it's serialization form should be
 changed, a new class called `StringBytesReader_3_5` (5 is the "minor" number of the version of the
 library, in which this change is made) should be created and put along with older class.
 - When the specification revision is updated (hence binary form could be completely changed), a
 new version of root implementation class, e. g. `VanillaChronicleMap_3_5` should be created and
 put along with older version, which should keep behave the same as before.

### Replication Protocol

The replication protocol might change between Chronicle Map versions with different "minor" number.
Chronicle Map nodes running different versions of the library shouldn't communicate to each other.
Though this might lead to failures, but not to data corruption on either of the nodes.

## The Specification Compatibility

The Chronicle Map Data Store Specification could be updated without changing the revision while it
remains backward compatible, i. e. the implementation of the newer version of the specification
could access and update any persisted Chronicle Map store, produced by an older version.
In addition, potential *concurrent* accessing implementations of older versions ("within" the same
revision) shouldn't lose correctness and corrupt the data.

If more substantial changes to the specification are required, the revision number should be
updated.

The binary form of Chronicle Map and algorithms could change between revisions to any degree, but
[guarantees](../spec/1-design-goals.md#guarantees) provided by the specification shouldn't be
weaken.
