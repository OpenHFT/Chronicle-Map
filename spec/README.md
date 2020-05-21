# The Chronicle Map Data Store Specification, revision 1

 1. [Design goals, assumptions and guarantees](1-design-goals.md)
 2. [Design overview](2-design-overview.md)
 3. [Memory layout](3-memory-layout.md)
   1. [Header fields](3_1-header-fields.md)
   2. [Lock structure and locking operations](3_2-lock-structure.md)
 4. [Key hashing and checksum algorithms](4-hashing-algorithms.md)
 5. [Initialization operations](5-initialization.md)
 6. [Query operations](6-queries.md)

## Rational behind this Specification

 - To provide the ability to write a small, drastically simplified Chronicle Map implementation in
 Java for a specific use-case. It could hard-code many aspects which the reference implementation
 has to handle as abstractions which are harder for the JVM to execute efficiently. Another way of
 improving performance is by weakening guarantees implied by the specification and the reference
 implementation.

 - To provide the ability to write a Chronicle Map implementation in a non-JVM language e.g. C, C++,
 C#, or Go. As Chronicle Map is designed for concurrent inter-process access, performance-critical
 operations on a Chronicle Map store could be done from a C++ process, while a concurrent process
 running the Java implementation is doing some duty tasks such as backup to a rational database.

 - This specification is the source of truth to reason about the Chronicle Map data store behaviour
 and correctness. If the reference implementation doesn't follow the specification, it is considered
 as an implementation bug.

 - To provide the ability for Chronicle Map adopters, database developers and researchers to study
 the Chronicle Map design and possibly draw their own conclusions about its behaviour, guarantees
 and efficiency. The reference implementation is quite complex so it's hard to understand how it
 works by solely reading the code, especially for people who don't know Java.

 - To keep the rationalization of the design decisions together in one place.
