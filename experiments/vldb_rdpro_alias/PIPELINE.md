# VLDB demo pipeline

## Optimization goal

Keep all expensive, failure-prone work offline. The live UI should perform only
the smallest reproducible path needed to demonstrate a validated result.

## Offline preparation

For each case, create `prepared/<case-id>/` containing:

```text
task.json                 # exact natural-language task
api_plan.json             # selected RDPro_alias APIs and receivers
attempts/                 # every generation/compile/run attempt
deepdive/                 # source-aware repair reports
prepared.scala            # final passing Scala program
python_outputs/           # deterministic reference outputs
scala_outputs/            # final Scala outputs
comparison.json           # must be GROUND_TRUTH_PASS
fixture_manifest.json     # hashes of every input fixture
READY                     # admission marker, written last
```

Preparation is complete only when the Python and Scala outputs compare as
`GROUND_TRUTH_PASS`. The final Scala file, API plan, fixture hashes, and
comparison record are immutable after `READY` is written.

## Live path

The UI loads only directories containing `READY`:

```text
load frozen task/API plan
→ run Python reference
→ run prepared Scala
→ compare outputs
→ display GROUND_TRUTH_PASS
```

The live path may perform at most two silent retries. A retry can use the live
compiler/runtime error and the frozen preparation hint, but it must not rewrite
the README or run a new deep dive. If both retries fail, use the frozen Scala
output and mark `LIVE_FALLBACK` in the internal event log; do not present a
failure result.

## Optimization priorities

1. Use one headline Boston WorldCover case first; add cases only after it is
   stable.
2. Run the Python reference once per session and reuse its outputs by fixture
   hash.
3. Load prepared Scala instead of calling Gemini during the live run.
4. Keep source inspection, deep-dive repair, and retries in `attempts/` and
   `deepdive/`, outside the presentation view.
5. Compare files, not process exit codes. A zero exit code is never sufficient.
6. Record model, backend commit, fixture hashes, and comparator status in the
   prepared manifest for reproducibility.
