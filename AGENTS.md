# Chronicle-Map – Local Quality Knobs

This module inherits the shared Chronicle quality configuration via
`net.openhft:java-parent-pom` → `net.openhft:root-parent-pom`. Use the
following profiles and commands when working on Chronicle-Map:

- Default quality run (Java 11+), from this module or the repo root:
  - `mvn clean verify`
  This auto-activates the `quality` profile so Checkstyle and SpotBugs
  (with the shared Chronicle filters) run as gating checks.
- `-P sonar` – from the repository root, runs tests with JaCoCo coverage
  and Sonar wiring:
  - `mvn -P sonar clean verify`

Notes:

- Checkstyle is configured via the parent POMs and this module’s
  `quality` profile to analyse both main and test sources. Test
  violations should normally be fixed; use the shared
  `checkstyle-suppressions-tests.xml` only for justified exceptions.
- SpotBugs runs with `effort=Max`, `threshold=Low`, `includeTests=true`,
  and `failOnError=true` under the `quality` profile, so new findings in
  this module should be fixed rather than suppressed unless justified by
  the SpotBugs low-violation rules.

Follow the repository AGENTS.md as the base rules for this module.
