# AGENTS.md

## Scope
- Chronicle Map provides off-heap persisted maps and replication.
- Performance and allocation behaviour are critical; keep hot paths lean.

## Build and test
- Preferred full check:
  - `mkdir -p logs`
  - `mvn verify -l logs/mvn-verify.log`
- Module-scoped example:
  - `mvn -pl <module> -am verify -l logs/mvn-verify.log`
- Test example:
  - `mvn -Dtest=ClassName test -l logs/mvn-test.log`
- Review logs:
  - `rg -n '^\[(WARNING|ERROR)\]|SLF4J\(W\)|\bWARNING:|\bwarning:' logs/mvn-verify.log`
- Do not commit logs/.

## Repo map
- `src/main/java` contains the main library code.
- `src/test/java` holds unit and integration tests.
- `benchmark/` contains JLBH benchmarks.
- `docs/` contains documentation and notes.

## Constraints
- Java baseline: 8 (avoid newer language features).
- Source files must stay ISO-8859-1 (code points 0-255). Prefer ASCII; avoid smart quotes and non-breaking spaces.
- Preserve public APIs and serialisation formats unless explicitly requested.
- Treat warnings as defects; keep logs clean.
- Avoid extra allocations or synchronisation on hot paths.
- Benchmarks are optional; do not change them unless requested.

## Docs and review checklist
- Keep docs, tests, and code in sync.
- For large mechanical changes, declare the transformation rule and keep it consistent.
- Add clarifying comments only when intent is non-obvious.

## References
- `OpenHFT/docs/Company-Wide-Tagging.adoc` for tagging and decision record templates.
