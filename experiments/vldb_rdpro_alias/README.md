# VLDB RDPro_alias demo

This worktree is the isolated VLDB demo project. It evaluates the RDPro_alias
backend without changing the RDPro/MDAnalysis experiment branches.

## Scope

- Backend source: `/Users/clockorangezoe/Documents/phd_projects/code/geoAI/RDPro_alias`
- Demo UI: `grail-agent/ui/grail_ui.py`
- Deterministic reference: `grail-agent/examples/python/worldcover_boston_zonal.py`
- Frozen fixtures: `grail-agent/examples/fixtures/vldb_data_manifest.json`
- Correctness comparator: `grail-agent/ui/output_compare.py`

The first live case is Boston WorldCover zonal composition. The alias-specific
loader under test is `geoTiffAsFloat`, with `geoTiff[Float]` as the explicit
baseline. Both paths must be compared against the same Python ground truth.

## Three assistance conditions

1. Generated RDPro README/API documentation.
2. README plus accumulated compiler/runtime errors and fix hints.
3. README plus verified RDPro_alias compatibility functions.

The live demo uses at most two direct code-repair retries. Deep source analysis
and documentation preparation happen offline and are saved as artifacts; they
are not mixed into the live timing.

## Phase order

```text
freeze backend and fixtures
→ verify alias signatures against RDPro_alias source/jars
→ prepare API notes and candidate Scala offline
→ deep-dive repair documented failure APIs offline
→ freeze prepared docs/fix hints
→ run Python ground truth
→ run Scala with 0–2 direct retries
→ compare outputs
→ record PASS / WRONG / UNAVAILABLE
```

## Deep-dive repair boundary

Deep-dive repair is an offline phase between the first candidate run and the
live demo. For each failed API, inspect the RDPro_alias source, signature,
tests, and actual compiler/runtime error. Save a repaired API note and fix hint
under `experiments/vldb_rdpro_alias/prepared/`, then freeze those artifacts.
The live loop consumes the frozen hints but does not rewrite documentation or
run a five-round repair cycle.

Admission validation is implemented by `prepared_gate.py`; only directories
passing `validate_case` should be offered by the live UI.

```text
offline candidate
  → source-aware deep dive
  → repaired API note + fix hint
  → frozen prepared artifact
  → live generation + 0–2 direct retries
```

## Backend compatibility gate

Do not mix `RDPro_alias` documentation with the original GRAIL Beast checkout.
Every alias used in a generated program must be present in the selected backend
source and compiled classpath. If an alias is unavailable, the run is recorded
as a backend compatibility failure rather than silently falling back.
