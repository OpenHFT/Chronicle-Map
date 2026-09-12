# Chronicle-Map #400 — controlled partial-removal investigation

Branch: `fix/Chronicle-Map-400-document-capacity-loss-limitation`
Date: 2026-08-22

## Scope and correction

The earlier experiment on this branch did **not** reproduce issue #400. It used integer keys,
4,096-byte values declared as 32 bytes, 1,000 entries, and `clear()` followed by re-add. That only
showed that a 128x sizing error can exhaust a map. It did not support the claim that sizing caused
#400, and its `reAddOK=false` row contradicted its universal reclamation verdict.

This replacement preserves the reported 61,500-entry capacity, fixed 500-byte `ByteBuffer` keys,
fixed 5,120-byte `ByteBuffer` values, exact average-size samples, and repeated removal of the first
7,000 traversal entries followed by refill. It uses a fixed data stream so the affected release,
current `ea`, and this branch receive identical keys and values.

It is important not to call this an exact reproduction of the original issue body:

- the issue body leaves segment count CPU-dependent and uses an unseeded `SecureRandom`;
- the issue body leaves segment tiering at its default (`true`);
- the associated Stack Overflow variant explicitly disables segment tiering;
- the local fixed-seed, tiering-enabled runs did not reproduce the reported failure in 30 rounds;
- the local initial footprint also differs from the reporter's output, so an unresolved
  configuration or environment difference remains.

The evidence below therefore establishes a mechanism for the tiering-disabled variant only. It
does not establish the cause of the original tiering-enabled report and does not show that issue
#400 is fixed.

## Committed harness

`src/test/java/net/openhft/chronicle/map/Issue400ChurnHarness.java` is an executable `main` program.
Its fixed parameters are:

- `entries(61_500)`;
- fixed 500-byte `ByteBuffer` keys;
- fixed 5,120-byte `ByteBuffer` values;
- matching `averageKey` and `averageValue` samples;
- remove the first 7,000 entries encountered by `ChronicleMap.forEachEntry`;
- refill to 61,500 or until a put fails;
- continue for 30 rounds or ten failures;
- deterministic SHA-256 counter-mode data with seed `0x400_500_5120_61500L`.

The harness accepts:

```text
<version-label> <allow-segment-tiering> <actual-segments|auto> <rounds> <output.csv>
```

`auto` preserves the issue body's CPU-dependent segment selection. A number pins the experiment
for cross-machine comparison. The output records every segment after every refill and removal:
entry count, used and capacity bytes, free chunks, chunk size, allocated tiers, off-heap bytes,
remaining auto-resizes, and the exact segment targeted by a failed put.

`run-issue-400-harness.sh` compiles this same committed source against whichever Chronicle Map
checkout is the current directory, applies the required JVM module options, and runs it. For
example:

```bash
# From the affected-version checkout, with JDK 8 selected:
JAVA_HOME=/path/to/jdk8 /path/to/pr/docs/research/run-issue-400-harness.sh \
  3.22ea6 true auto 30 /tmp/chronicle-map-400-3.22ea6-auto-true.csv
JAVA_HOME=/path/to/jdk8 /path/to/pr/docs/research/run-issue-400-harness.sh \
  3.22ea6 false auto 30 /tmp/chronicle-map-400-3.22ea6-auto-false.csv

# Repeat from current ea and the PR checkout with their selected JDK:
/path/to/pr/docs/research/run-issue-400-harness.sh \
  ea true auto 30 /tmp/chronicle-map-400-ea-auto-true.csv
/path/to/pr/docs/research/run-issue-400-harness.sh \
  ea false auto 30 /tmp/chronicle-map-400-ea-auto-false.csv
```

The summarized failure rows are in `chronicle-map-400-churn-matrix-results.csv`. The complete
64-segment snapshot at the first current-`ea` tiering-disabled failure is in
`chronicle-map-400-first-failure-segments.csv`. Raw-output line counts and SHA-256 receipts are in
`chronicle-map-400-run-receipts.csv`.

## Version matrix

The same harness source was compiled and run against:

| label | code | JDK | tiering | effective segments | result |
|---|---|---:|---:|---:|---|
| affected | `chronicle-map-3.22ea6` (`616b31df`) | 8 | `true` | 64 | 30 rounds; no failure |
| current base | `ea` (`0c25269d`) | 21 | `true` | 64 | 30 rounds; no failure |
| PR branch | `444eaa5c` plus this working-tree patch | 21 | `true` | 64 | 30 rounds; no failure |
| affected variant | `chronicle-map-3.22ea6` | 8 | `false` | 64 | first failure round 7; ten by round 22 |
| current variant | `ea` | 21 | `false` | 64 | identical numeric result |
| PR variant | branch | 21 | `false` | 64 | identical numeric result |

Pinning 32 segments changed the tiering-disabled failure sizes but not the conclusion: all three
versions again matched, while all three tiering-enabled controls completed 30 rounds. This
sensitivity is why segment count is now an explicit recorded input.

## Tiering-disabled observation

With 64 segments and tiering disabled, the first six refills reach 61,500. The first failure occurs
during round 7 at map size 58,438. Later failures occur at 51,459, 44,464, 39,465, 32,492, 25,504,
18,897, 11,906, and 5,809. After round 15 removal the map reaches zero and can fill again; the
failure recurs in round 22 at size 59,104.

The first failed put targets segment 23. That segment has reached 1,638 entries, but still reports
13,244 free 256-byte chunks (3,390,464 free bytes) in its only tier. Other segments contain as few
as 44 entries and as many as 48,312 free chunks. The immediate limit in this variant is therefore
per-segment entry capacity, not under-declared key/value sizes or exhaustion of all declared bytes.

The fixed traversal removal repeatedly drains the same range of segments, while new keys refill
all segments. With no extra segment tiers permitted, retained segments accumulate entries until
one reaches its fixed per-tier entry limit. Capacity in drained segments cannot be transferred to
it.

## Conclusions and non-conclusions

The evidence supports these statements:

1. The earlier under-sized 44-put scenario was not a reproduction of #400.
2. Exact key/value sizing does not prevent the tiering-disabled partial-churn failure.
3. `entries()` is a whole-map sizing input, not an insertability guarantee for every per-segment
   occupancy distribution when tier growth is unavailable.
4. The affected tag, current `ea`, and this branch behave identically in these fixed-seed runs.
5. Tiering avoids this particular failure in the bounded 30-round controls.

The evidence does **not** reproduce the original tiering-enabled issue body, establish why the
reporter's tier budget was exhausted, prove general chunk/tier reclamation correctness, make
`clear()` equivalent to partial churn, or show that issue #400 is fixed. The PR must not use a
closing keyword or present this research as a fix. A future allocator investigation should retain
this harness as one acceptance workload and first resolve the reporter/local configuration and
footprint difference.
