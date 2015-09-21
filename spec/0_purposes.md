# Purposes of Chronicle Map specification

 - To give ability to write a small, drastically simplified Chronicle Map implementation in Java,
 for a specific use-case. It could hard-code many aspects, which the reference implementation have
 to handle as abstractions, that is harder for JVM to execute efficiently. Another way of improving
 performance is weakening guarantees, implied by the specification and the reference implementation.

 - To give ability to write a Chronicle Map implementation in a non-JVM language, C++/C#/whatever.
 As Chronicle Map is designed for concurrent inter-process access, performance-critical operations
 on a Chronicle Map instance could be done from a C++ process, while a concurrent Java process
 is doing some duty tasks such as backup to a rational database.

 - Chronicle Map specification is the source of truth to reason about the Chronicle Map data
 structure behaviour and correctness. If the reference implementation doesn't follow the
 specification, it is considered as an implementation bug.

 - To give ability for Chronicle Map adopters, database developers and researchers to study
 Chronicle Map design and possibly draw own conclusions about it's behaviour, guarantees and
 efficiency. The reference implementation is quite complex and it's hard to understand how does it
 work for people who are not familiar with the code base, especially for people who doesn't know
 Java.

 - To put rationalization of the design decisions together in one place.