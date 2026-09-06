# Ray Fabric Scenario Test Dataset

This pack is designed to test Ray as a Senior Data Engineer, not just a code generator.

## Folders

- `ray_input/` — Give these files to Ray.
- `sentinel_expected/` — Keep these hidden from Ray; Sentinel uses them for evaluation.
- `TEST_PROMPTS.json` — Ready-to-use prompts for scenarios T01, T03, T04, T05 and T06.

## Recommended order

1. T01 — Clarification
2. T03 — SQL / Gold aggregation
3. T04 — PySpark / Silver cleansing
4. T05 — Data discrepancy investigation
5. T06 — Business-rule ambiguity

## Important

Do not give `sentinel_expected/ground_truth.json` to Ray, otherwise the discrepancy and quality issues are no longer a valid blind test.
