# Chronicle Map Memory Layout

Newly created Chronicle Map (or just mapped from an already existing persistence file) takes one big
continuous block of memory. It's structure, from lower addresses to higher:

 1. Self-bootstrapping header
 2. Global mutable state
 3. Alignment to the next page boundary by addresses (page size of the current memory mapping),
 if a new Chronicle Map is created, if the existing one (e. g. persisted) is loaded, the alignment
 is specified in the global mutable state (however, specifically this field of global mutable state
 is actually immutable).

 > The purpose of this alignment is to minimize the number of pages spanned by the following segment
 > headers area. Segment headers area is frequently accessed and updated, so we want to minimize the
 > number of TLB cache entries it almost constantly reside and the number of pages to flush to disk,
 > if the Chronicle Map is persisted.

 > Need to store the alignment, because other accessing processes and/or implementations of
 > Chronicle Map may use different page size for memory mapping.

 4. Segment headers area
 5. Main segments area
 6. Zero to several *extra tier bulks*. If the Chronicle Map is persisted to a file, each extra tier
 bulk is pre-aligned to the granularity of offsets in files supported by memory mapping facility in
 the operating system running at the moment of extra tier bulk allocation, then these alignments are
 stored for each extra tier bulk in the global mutable state.

 > The reason to make alignment before each extra tier bulk is different from that for alignment
 > before segment headers area. Extra tier bulks are allocated in runtime, there are zero bulks in
 > a newly created Chronicle Map. If the Chronicle Map is persisted, it should extend the persisted
 > file and map the newly allocated file space into memory. In processes which will load this
 > Chronicle Map later the already existing Chronicle Map space and the new extra tier bulk will
 > be served by a single memory mapping, hence continuous range of addresses. But in the processes
 > *currently working* with this Chronicle Map (including the process triggered extra tier bulk
 > allocation itself), i. e. already using a *shorter* memory-mapping, this new extra tier bulk
 > should be loaded as a *separate* memory mapping. This separate memory mapping with offset in
 > the persistence file, should have the offset aligned accordingly.

 > Granularity of offsets in files, supported by memory mapping facility is equal to *native* page
 > size in Linux, i. e. most likely 4KB, and "file allocation granularity" in Windows, i. e. most
 > likely 64KB.

## Self-Bootstrapping Header

It's structure and initialization order is described in [Self Bootstrapping Data 1.0](
https://github.com/OpenHFT/RFC/blob/master/Self-Bootstrapping-Data/Self-Bootstraping-Data-0.1.md)
specification. It could be in Binary Wire or Text Wire formats (see details in the Self
Bootstrapping Data specification). Once created, this header is never changed. It contains all
configurations, immutable for Chronicle Map during the instance "lifetime": number of segments,
various sizes, offsets, etc. See the specification of the fields on [Map Header
Fields](3_1-map-header-fields.md) page.

## Global Mutable State

