# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Chronicle Map is a high-performance, off-heap, persisted key-value store designed for low-latency and multi-process applications. It provides concurrent inter-process access via memory-mapped files.

## Build and Test Commands

```bash
# Full verification (preferred)
mkdir -p logs
mvn verify -l logs/mvn-verify.log

# Run a single test
mvn -Dtest=ClassName test -l logs/mvn-test.log

# Review logs for issues
rg -n '^\[(WARNING|ERROR)\]|SLF4J\(W\)|\bWARNING:|\bwarning:' logs/mvn-verify.log
```

Do not commit the `logs/` directory.

## Architecture

### Core Type Hierarchy

- `ChronicleHash<K>` - Base interface for both Map and Set
  - `ChronicleMap<K,V>` - Concurrent off-heap map extending `ConcurrentMap`
  - `ChronicleSet<K>` - Set implementation (Map with zero-sized values)

### Key Packages

- `net.openhft.chronicle.map` - ChronicleMap interface and ChronicleMapBuilder
- `net.openhft.chronicle.set` - ChronicleSet interface and ChronicleSetBuilder
- `net.openhft.chronicle.hash` - Common hash infrastructure (ChronicleHash, contexts, entries)
- `net.openhft.chronicle.hash.impl` - Core implementation (VanillaChronicleHash, segment handling, hash tables)
- `net.openhft.chronicle.hash.serialization` - Serialization interfaces (SizedReader, SizedWriter, DataAccess)
- `net.openhft.chronicle.hash.replication` - Multi-master replication support
- `net.openhft.chronicle.map.impl` - Generated query/iteration contexts (CompiledMapQueryContext, etc.)

### Implementation Classes

- `VanillaChronicleHash` - Abstract base handling memory mapping, segments, locking
- `VanillaChronicleMap` / `ReplicatedChronicleMap` - Concrete map implementations
- `CompactOffHeapLinearHashTable` - Hash table implementation (int/long variants)
- `SegmentStages` / `HashEntryStages` - Entry and segment lifecycle management

### Context System

Query and iteration operations use context objects that manage entry access, locking, and memory:
- `MapQueryContext` / `MapIterationContext` - For maps
- `SetQueryContext` - For sets
- Contexts handle read/update/write locks at segment granularity

## Constraints

- **Java 8 baseline** - Avoid newer language features
- **ISO-8859-1 encoding** - Source files must use code points 0-255; prefer ASCII
- **Binary compatibility** - Preserve public APIs and serialization formats
- **Performance critical** - Avoid allocations and synchronization on hot paths
- **Warnings as defects** - Keep build logs clean

## References

- `spec/` - Formal Chronicle Map data store specification
- `docs/` - User documentation (tutorial, FAQs, features)
- `benchmark/` - JLBH benchmarks (do not modify unless requested)
