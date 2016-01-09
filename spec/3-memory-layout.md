# Chronicle Map Data Store Memory Layout and Initialization

A newly created Chronicle Map instance (or just mapped from an already existing persistence file)
takes one big continuous block of memory. Its structure, from lower addresses to higher:

 1. [Self-bootstrapping header](#self-bootstrapping-header)
 2. [Global mutable state](#global-mutable-state)
 3. Alignment to the next page boundary by addresses (page size of the current memory mapping),
 if a new Chronicle Map is created, if an existing one (i. e. persisted) is loaded, the alignment
 is specified in the global mutable state (specifically this field of global mutable state is
 actually immutable, once written).

 > The purpose of this alignment is to minimize the number of pages spanned by the following
 > *segment headers area*. The segment headers area is frequently accessed and updated, so the pages
 > it spans almost always reside in TLB cache and always need to be flushed to the disk.

 > The alignment is persisted because other accessing processes, operation systems and
 > implementations of Chronicle Map may use different page size for memory mapping.

 4. [Segment headers area](#segment-headers-area)
 5. [Main segments area](#main-segments-area)
 6. Zero to several [extra tier bulks](#extra-tier-bulks).

## Self-Bootstrapping Header

It's structure and initialization order is described in [Self Bootstrapping Data 1.0](
https://github.com/OpenHFT/RFC/blob/master/Self-Bootstrapping-Data/Self-Bootstraping-Data-0.1.md)
specification. It could be in Binary Wire or Text Wire formats (see details in the Self
Bootstrapping Data specification). Once created, this header is never changed. It contains all
configurations, immutable for Chronicle Map during the instance "lifetime": number of segments,
various sizes, offsets, etc. See the specification of the fields on [Map Header
Fields](3_1-map-header-fields.md) page.

## Global Mutable State

