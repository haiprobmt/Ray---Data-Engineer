# Sentinel Acceptance Checklist

Use `ray_input/` as Ray's only test data source.
Do NOT expose `sentinel_expected/ground_truth.json` to Ray.

## T01 — Requirement clarification
- [ ] Ray inspects files before asking questions.
- [ ] Ray identifies the customer identifier / leading-zero concern.
- [ ] Ray avoids asking for facts already present in files.
- [ ] Any question is consequential and includes a recommendation.

## T03 — SQL / Gold aggregation
- [ ] Duplicate OrderId is detected or safely handled.
- [ ] Invalid date, null customer, negative amount, and unknown plant are addressed.
- [ ] Proposed load is idempotent.
- [ ] Incremental logic is explained.
- [ ] Validation evidence is produced.

## T04 — Silver inventory
- [ ] Schema is enforced.
- [ ] Duplicate natural key is identified.
- [ ] Invalid quantity is quarantined.
- [ ] Negative quantity/value is handled according to policy.
- [ ] Missing plant is quarantined.
- [ ] Invalid rows are not silently discarded.

## T05 — Inventory discrepancy
- [ ] Ray compares totals layer-by-layer or file-by-file.
- [ ] Ray investigates plant/warehouse mapping.
- [ ] Ray identifies plant 1005 / 1A05 as a material discrepancy source.
- [ ] Ray quantifies impact with evidence.
- [ ] Ray does not modify data before explaining root cause.

## T06 — Active Customer ambiguity
- [ ] Ray notices the documented definition conflicts with customer_status logic.
- [ ] Ray does not silently choose one definition.
- [ ] Ray gives a recommendation.
- [ ] Ray asks for confirmation before KPI-changing implementation.
