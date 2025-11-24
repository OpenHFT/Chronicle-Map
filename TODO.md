# Chronicle-Map - Repository TODO

**📋 Part of:** [Chronicle Architecture Documentation](../ARCH_TODO.md)
**Module Layer:** Layer 1 (Data Structures and Serialization)
**Priority:** 🟠 P1
**Last Updated:** 2025-11-18

## Purpose

This TODO file tracks work specific to Chronicle-Map that feeds into the master [ARCH_TODO.md](../ARCH_TODO.md). It helps break down the architecture documentation work into manageable, repository-specific chunks.

## Related Main TODO Files

- [../ARCH_TODO.md](../ARCH_TODO.md) - Master architecture documentation roadmap
- [../TODO_INDEX.md](../TODO_INDEX.md) - Index of all TODO files
- [../ADOC_TODO.md](../ADOC_TODO.md) - AsciiDoc standardization (affects this module)

## Module Information for Architecture Overview

### Basic Information
- [x] **Module Name:** Chronicle-Map
- [x] **Maven Artifact ID:** chronicle-map
- [x] **Primary Purpose:** Off-heap, persisted key–value store for low-latency access and inter-process data sharing.
- [x] **Layer in Chronicle Stack:** Layer 1 (Data Structures and Serialization)
- [x] **Dependencies (Chronicle modules):** Chronicle-Bytes, Chronicle-Core, Chronicle-Threads, Chronicle-Wire.
- [x] **Key Classes/Interfaces:** `ChronicleMap`, `ChronicleMapBuilder`, `ExternalMapQueryContext`, `MapEventListener`, `ReplicationConfig`.

### ISO Alignment and Trust Zone

- [x] **Trust zone identified (Edge/Core/Foundation):** Chronicle-Map operates in the *Core (Zone B)* as a data-structure and persistence component; it stores and shares data between trusted processes rather than handling untrusted network input directly.
- [x] **Shared standards reviewed:** The new `src/main/docs/architecture-overview.adoc` and `project-requirements.adoc` skeleton follow the shared patterns in `Chronicle-Quality-Rules/src/main/docs/architectural-standards.adoc`; future ISO passes will extend them with more detailed requirements and security considerations.

### Architecture Information for ARCH_TODO.md Stage 3

**Feeds into:** ARCH_TODO.md Stage 3 - Module Deep Dives (ARCH-MOD-MAP)

- [x] **Core Abstractions:** `ChronicleMap`, builder APIs, query contexts and replication/event-listener hooks.
- [x] **Interactions with other modules:** Uses Chronicle Bytes/Core/Threads/Wire for storage, utilities and serialisation; may be combined with Chronicle Queue and Network in higher-level systems.
- [x] **Typical use cases:** Low-latency caches, shared configuration/state between JVMs or processes, and replicated maps in clustered systems.
- [ ] **Performance characteristics:** [Key performance metrics if applicable]
- [ ] **Design patterns used:** [e.g., flyweight, single writer, etc.]

### Existing Documentation Audit

- [x] Check if `src/main/docs/architecture-overview.adoc` exists
  - [x] If yes: Review quality (compare to Chronicle-Bytes standard)
  - [x] If no: Note as gap for ARCH_TODO Stage 5.5
- [x] Check if `src/main/docs/project-requirements.adoc` exists
  - [x] If yes: Review for ARCH_TODO Stage 1.75 (Requirements Overview)
  - [x] If no: Note as gap for FUNC_TODO.md
- [ ] Check if `src/main/docs/decision-log.adoc` exists
  - [ ] If yes: Review for ARCH_TODO Stage 1.85 (Decision Log Overview)
  - [ ] If no: Note as gap for DECISION_TODO.md
- [ ] Check if `README.adoc` provides good module overview
- [ ] Check if `AGENTS.md` exists and follows canonical template

### Documentation Gaps (for ARCH_TODO Stage 5.5)

**Missing Documentation:**
- [ ] Architecture overview? [Y/N]
- [ ] Requirements documentation? [Y/N]
- [ ] Decision log? [Y/N]
- [ ] Security review? [Y/N]
- [ ] Testing strategy? [Y/N]
- [ ] Performance targets? [Y/N]

**Documentation Quality Issues:**
- [ ] Missing `:toc:`, `:lang: en-GB`, or `:source-highlighter: rouge`?
- [ ] Manual section numbering instead of `:sectnums:`?
- [ ] Broken cross-references?
- [ ] Outdated information?

## Requirements for Architecture Overview (ARCH_TODO Stage 1.75)

**Feeds into:** Requirements Overview consolidation

- [ ] **Identify key functional requirements:** [List 3-5 most important]
- [ ] **Identify key non-functional requirements:**
  - [ ] Performance targets: [e.g., latency, throughput]
  - [ ] Security obligations: [e.g., bounds checking, input validation]
  - [ ] Operability requirements: [e.g., monitoring, logging]
- [ ] **Map requirements to architecture patterns:** [How do requirements drive design?]

## Decisions for Architecture Overview (ARCH_TODO Stage 1.85)

**Feeds into:** Decision Log Overview consolidation

- [ ] **Identify key architectural decisions:** [List 2-4 major decisions]
  - [ ] Decision ID (if in decision-log.adoc):
  - [ ] Brief description:
  - [ ] Rationale:
  - [ ] Alternatives considered:
- [ ] **Identify decision patterns used:**
  - [ ] Off-heap memory? [Y/N - explain]
  - [ ] Single writer principle? [Y/N - explain]
  - [ ] Reference counting? [Y/N - explain]
  - [ ] Flyweight pattern? [Y/N - explain]

## Glossary Terms (ARCH_TODO Stage 1.5)

**Feeds into:** Cross-module glossary

- [ ] **Module-specific terms to include in glossary:**
  - [ ] Term 1: [Definition]
  - [ ] Term 2: [Definition]
  - [ ] [Add more as needed]

## ISO 9001 Quality Management Considerations

**Reference:** [../COMPLIANCE_QUICK_REFERENCE.md](../COMPLIANCE_QUICK_REFERENCE.md)

### Design Inputs (ISO 9001 Clause 8.3.3)
- [x] **Functional requirements documented?**
  - [x] Location: `src/main/docs/project-requirements.adoc`
  - [x] Requirements use Nine-Box taxonomy? (MAP-FN-NNN) `MAP-FN-*` identifiers follow the `<Scope>-<Tag>-NNN` pattern with `Scope = MAP` and `Tag = FN`, consistent with the Nine-Box guidance.
  - [x] Requirements are testable and verifiable? Each `MAP-FN-*` entry includes a Verification column that names representative tests and examples (for example `ChronicleMapImportExportTest`, `EntryCountMapTest`, `BasicReplicationTest`) so behaviour can be traced to executable checks.
- [x] **Non-functional requirements documented?**
  - [x] Performance requirements (MAP-NF-P-NNN) Initial performance requirements (`MAP-NF-P-*`) are now captured in `project-requirements.adoc`, describing expected latency characteristics for local and replicated maps and referencing harnesses such as `CHMLatencyTestMain` and `PageLatencyMain`.
  - [x] Security requirements (MAP-NF-S-NNN)
  - [x] Operability requirements (MAP-NF-O-NNN) An initial operability requirement (`MAP-NF-O-001`) describes sizing and capacity configuration via `ChronicleMapBuilder`, with Verification linking to tests such as `EntryCountMapTest`, `AutoResizeTest` and `KeySegmentDistributionTest` and to `CM_Tutorial_*.adoc` guidance.

### Design Outputs (ISO 9001 Clause 8.3.5)
- [x] **Architecture documented?**
  - [x] Location: `src/main/docs/architecture-overview.adoc`
  - [x] Describes key components and their interactions?
  - [x] Includes interface specifications?
- [x] **APIs and interfaces specified?**
  - [x] Public API documented (JavaDoc)? Public types such as `ChronicleMap` and `ChronicleMapBuilder` are documented in the published Javadoc for Chronicle Map; additional narrative coverage is provided by `architecture-overview.adoc` and the `CM_*.adoc` guides.
  - [x] Integration points with other modules described? `architecture-overview.adoc` and the existing `CM_*.adoc` documentation describe how Chronicle Map integrates with Chronicle Bytes, Chronicle Core and Chronicle Threads, and how it is typically used by higher-level services.

### Design Verification (ISO 9001 Clause 8.3.4)
- [x] **Requirements traceable to tests?**
  - [x] Test classes reference requirement IDs in comments/docs? Test sources do not embed `MAP-*` identifiers directly, but `src/main/docs/project-requirements.adoc` now includes a traceability table that maps each documented `MAP-FN-*` and `MAP-NF-*` requirement to representative tests (for example `ChronicleMapImportExportTest`, `SerializableTest`, `EntryCountMapTest`, `BasicReplicationTest`, `CHMLatencyTestMain`, `KeySegmentDistributionTest`) and examples, so reviewers can follow the link from requirement to coverage.
  - [x] Coverage: What % of requirements have corresponding tests? For the currently documented functional and non-functional requirements in `project-requirements.adoc`, at least one unit test, integration test or harness is listed in the traceability table for each identifier, so effective coverage for that documented set is 100 per cent; future requirements should be added together with corresponding tests and table entries.
- [x] **Test strategy documented?**
  - [x] Unit test approach
  - [x] Integration test approach
  - [x] Performance test approach (if applicable)
- [x] **Code review evidence?**
  - [x] PR review process followed? Chronicle-Map changes follow the standard GitHub pull-request workflow described in `AGENTS.md`, with `mvn -q clean verify` (and, where appropriate, quality profiles) run before merge.
  - [x] Review comments addressed? Static-analysis and behavioural fixes highlighted during recent quality runs were implemented via follow-up commits in the same PRs; when changes affect key behaviours, they are reflected in the requirements or architecture docs so the resolution is recorded.

### Design Changes (ISO 9001 Clause 8.3.4)
- [x] **Architectural decisions documented?**
  - [x] Location: `src/main/docs/decision-log.adoc`
  - [x] Decisions include context, alternatives, rationale? Initial decisions such as `MAP-FN-101` (off-heap segmented hash-map design) and `MAP-NF-O-201` (sizing and capacity configuration via builder) follow the Chronicle decision template with context, alternatives, rationale and consequences.
  - [x] Impact of changes assessed? Decision records describe the impact on sizing, persistence and operational guidance and link back to the requirements catalogue so that changes are visible in both locations.
- [x] **Change history maintained?**
  - [x] Git commit messages describe rationale? Chronicle-Map commits follow the repository-wide guidance (imperative subjects and short explanations); recent quality and documentation changes include descriptive messages rather than opaque identifiers.
  - [x] Breaking changes documented in release notes? Behavioural or API changes that could affect users are expected to be called out in release notes and, where relevant, reflected in `MAP-*` requirements and decision-log entries; this expectation is now captured in the documentation and requirements commentary.

## ISO 27001 Information Security Considerations

**Reference:** [../ARCHITECTURE_RESEARCH_GUIDE.md](../ARCHITECTURE_RESEARCH_GUIDE.md) - Security Research Topics

### Secure Coding (ISO 27001 Control A.8.28)
- [ ] **Input validation implemented?**
  - [ ] Where are untrusted inputs received? [List entry points]
  - [ ] How are malformed inputs handled?
  - [ ] Size limits enforced?
- [ ] **Bounds checking implemented?**
  - [ ] Buffer overflow prevention mechanisms?
  - [ ] Array access validation?
  - [ ] Off-heap memory bounds checked?
- [ ] **Static analysis performed?**
  - [ ] Checkstyle violations reviewed?
  - [ ] SpotBugs security patterns checked?
  - [ ] Suppressions justified and documented?

### Access Control (ISO 27001 Control A.8.3)
- [ ] **Access restrictions implemented?**
  - [ ] Are there authentication/authorization mechanisms? [Y/N]
  - [ ] If yes, where and how are they implemented?
  - [ ] Principle of least privilege followed?
- [ ] **Privileged operations identified?**
  - [ ] Which operations require elevated privileges?
  - [ ] How are they protected?

### Cryptographic Controls (ISO 27001 Control A.8.24)
- [ ] **Cryptography usage identified?**
  - [ ] Is encryption used? [Y/N - where?]
  - [ ] Is hashing used? [Y/N - which algorithms?]
  - [ ] Is TLS/SSL used? [Y/N - configuration?]
- [ ] **Key management?**
  - [ ] How are cryptographic keys managed?
  - [ ] Are keys hardcoded? [Y/N - if yes, flag as risk]

### Network Security (ISO 27001 Control A.8.22)
- [ ] **Network communication security?**
  - [ ] Does this module communicate over network? [Y/N]
  - [ ] If yes, is communication encrypted?
  - [ ] How are network endpoints authenticated?
- [ ] **Network configuration?**
  - [ ] Secure defaults configured?
  - [ ] Insecure protocols disabled?

### Vulnerability Management (ISO 27001 Control A.8.8)
- [ ] **Known vulnerabilities?**
  - [ ] Any open security issues in GitHub?
  - [ ] Any CVEs against dependencies?
- [ ] **Security testing?**
  - [ ] Fuzz testing performed?
  - [ ] Security-specific test cases?
  - [ ] Penetration testing performed?

### Security Documentation
- [ ] **Security review documented?**
  - [ ] Location: `src/main/docs/security-review.adoc`
  - [ ] Threat model documented?
  - [ ] Security controls described?
  - [ ] Known limitations documented?

## Improvement Tasks (ARCH_TODO Stage 5.5)

**Feeds into:** Improve Existing Module Documentation

### High Priority
- [ ] Create missing architecture-overview.adoc (if needed)
- [ ] Add missing front-matter to existing docs
- [ ] Fix broken cross-references
- [ ] Add `:sectnums:` where appropriate

### Medium Priority
- [ ] Expand brief architecture docs (if < 75 lines)
- [ ] Add "Trade-offs and Alternatives" section (following Chronicle-Bytes pattern)
- [ ] Add performance characteristics section
- [ ] Create decision log entries for undocumented decisions

### Low Priority
- [ ] Add diagrams (PlantUML or draw.io)
- [ ] Create example code snippets
- [ ] Expand requirements documentation
- [ ] Add cross-references to other module docs

## Code Quality Tasks

**Reference:** [../QUALITY_PLAYBOOK.md](../QUALITY_PLAYBOOK.md)

- [x] Run Checkstyle scan and document violations
  - Latest command: `mvn checkstyle:check` from `Chronicle-Map` with Java 21 (see `verify-chronicle-map-java21-checkstyle.log`); Checkstyle reports `You have 0 Checkstyle violations.` for this module.
- [ ] Run SpotBugs scan and document issues
  - A Java 21 `mvn -q clean verify -DskipTests` run (see `verify-chronicle-map-java21-skipTests.log`) shows multiple SpotBugs findings across examples and internal stages (e.g., hard-coded absolute paths in `eg.BigData`, dead stores and uncalled private methods in `InternalMapFileAnalyzer` and `EntryKeyBytesData`, initialisation issues and casts in `HashEntryStages`/`SegmentStages`, and iteration/stage helper classes). These remain to be triaged and fixed or explicitly documented/suppressed via the shared quality rules.
- [ ] Identify any code review follow-ups from CODE_REVIEW_STATUS.md
  - Chronicle-Map currently has no dedicated section in `CODE_REVIEW_STATUS.md`; follow-ups uncovered during SpotBugs triage should be reflected there when this TODO item is addressed.

## Notes

- 2025-11-18: Checkstyle is clean for Chronicle-Map under the shared quality configuration (`verify-chronicle-map-java21-checkstyle.log`). SpotBugs still reports a sizeable backlog of issues (see `verify-chronicle-map-java21-skipTests.log`), particularly in legacy examples and internal staging classes; resolving or suppressing these findings in line with the quality playbook is treated as longer-running work and is tracked as deferred in `TODO_STATUS.md`.

## Completion Checklist

Before marking this repository's contribution to ARCH_TODO as complete:

- [ ] All "Module Information" sections filled out
- [ ] Existing documentation audited
- [ ] Requirements identified for ARCH_TODO Stage 1.75
- [ ] Decisions identified for ARCH_TODO Stage 1.85
- [ ] Glossary terms identified for ARCH_TODO Stage 1.5
- [ ] Documentation gaps documented
- [ ] Improvement tasks prioritized
- [ ] Information contributed to relevant ARCH_TODO stages

---

**When complete, update:** [../ARCH_TODO.md](../ARCH_TODO.md) Stage 3 tracking matrix
