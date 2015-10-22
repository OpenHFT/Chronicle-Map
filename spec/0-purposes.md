# Rational behind Chronicle Map Specification

 - To provide the ability to write a small, drastically simplified Chronicle Map implementation in Java
 for a specific use-case. It could hard-code many aspects which the reference implementation has
 to handle as abstractions which are harder for the JVM to execute efficiently. Another way of improving
 performance is by weakening guarantees implied by the specification and the reference implementation.

 - To provide the ability to write a Chronicle Map implementation in a non-JVM language e.g. C++, C#.
 As Chronicle Map is designed for concurrent inter-process access, performance-critical operations
 on a Chronicle Map instance could be done from a C++ process, while a concurrent Java process
 is doing some duty tasks such as backup to a rational database.

 - Chronicle Map specification is the source of truth to reason about the Chronicle Map data
 structure behaviour and correctness. If the reference implementation doesn't follow the
 specification, it is considered as an implementation bug.

 - To provide the ability for Chronicle Map adopters, database developers and researchers to study
 Chronicle Map design and possibly draw their own conclusions about it's behaviour, guarantees and
 efficiency. The reference implementation is quite complex so it's hard to understand how it
 works by just reading the code, especially for people who don't know
 Java.

 - To keep the rationalization of the design decisions together in one place.
