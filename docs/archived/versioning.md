# Chronicle Map 3 Versioning

## The Java implementation Versioning

3.minor.patch

"Minor" number should be updated, when

 - A feature, or public API method is added.
 - Behaviour of `ChronicleMap`, `ChronicleMapBuilder` or any other class or method or subsystem
 is semantically changed.
   - For example, when configuration heuristics in `ChronicleMapBuilder` are improved, and
   `ChronicleMapBuilder` configured the same way constructs a `ChronicleMap` with different
   number of segments, or segment's capacity, etc.
 - Any dependency is updated, even if only "patch" number is updated, e. g. `chronicle-bytes`
 dependency is updated from `1.1.1` to `1.1.2`. This rule doesn't apply to dependencies to the
 umbrella POMs which merely determine versions of other dependencies (namely `chronicle-bom` and
 `third-party-bom`), if the versions of actual dependencies doesn't change between those versions
 of umbrella POMs. Also, this rule doesn't apply to dependencies with `test` and `provided` Maven
 scopes.
 - Bug fixing, performance optimization or feature addition requires to change the binary or
 serialized form of Chronicle Map data store (therefore revise the Chronicle Map data store
 specification, see below), or the binary replication protocol, or the serialized form of some
 marshaller classes, that requires to [create new versions of classes](compatibility.md).

"Minor" number shouldn't be updated, i. e. the next release could update only the "patch" number,
when

 - A bug fixed, in a way that doesn't make any working code to change it's behaviour semantically.
 - Performance optimized: some working code changes it's performance characteristics,
 but not semantics

During development after the release of the version `3.y.z` the library should be versioned
`3.y.<x+1>-SNAPSHOT`. As soon as changes made are substantial enough for the next released version
to update the "minor" number, the version should be changed to `3.<y+1>.0-SNAPSHOT`, *in the same
commit* these changes are made.

Suffixes `-alpha`, `-beta`, `-rc` etc. mean this is a pre-production release.

## The Specification Versioning

The first Chronicle Map 3 production release designates the Chronicle Map data store specification,
revision 1.

When the specification should be updated in a way that is not [backward compatible](
compatibility.md#the-specification-compatibility), the revision should be updated (revision 2, 3,
...) and the Java implementation of this revision should have "minor" number updated in it's
version.

During the development after the release of the revision `x`, while the specification is backward
compatible, it should remain versioned `x`. In the same commit where the specification becomes
incompatible, it's revision should be set to `<x+1>-pre` until the next production release.

Releases and updates to the "minor" number in the library version should be synchronized with
updates to the specification revision number.
