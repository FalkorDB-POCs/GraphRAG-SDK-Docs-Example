# Structured inputs for the hybrid docs demo

These CSVs are ingested with GraphRAG-SDK structured mappings (`Table` /
`RecordMapping`) into the **same FalkorDB graph** as the PDFs under `docs/`.

## Design rules

1. **`practice_name` / `metric_name` / `scenario_name` values reuse PDF wording**
   so entity resolution can bridge structured rows and prose mentions via
   `key` + `name` → `alias_ids` and `finalize()`.
2. **Numeric columns are typed** (`FLOAT` / `INTEGER`) so text-to-Cypher can
   aggregate them (`enable_cypher=True`).
3. Values are aligned to figures discussed in:
   - `docs/2603.20674v1.pdf` — Carbon Farming survey (AWD, SRI, GCTB, MRV)
   - `docs/2603.23825v1.pdf` — China WTO accession / trade & innovation
   - `docs/2603.25874v1.pdf` — carbon–electricity market design

## Files

| File | Entity label | Bridge names |
| --- | --- | --- |
| `mitigation_practices.csv` | `MitigationPractice` | Alternate Wetting and Drying (AWD), System of Rice Intensification (SRI), Green Cane Trash Blanketing (GCTB), Carbon Farming |
| `trade_shock_metrics.csv` | `TradeMetric` | Iceberg trade cost reduction…, CES parameters, firm exit probability, processing-trade robustness |
| `electricity_carbon_scenarios.csv` | `PolicyScenario` | Austria/Germany 2025 baselines, hard threshold, linear ramp, settlement deduction |

## Built-in engine

GraphRAG-SDK structured ingestion ships **`CsvRecordLoader` only** (comma /
semicolon / tab / pipe). Other engines need a custom `RecordLoaderStrategy`.

## Scale note

For large-table simulation the three CSVs are expanded **100×** from the seed
rows (seed rows kept at the top with stable IDs used by gold checks):

| File | Seed rows | Scaled rows |
| --- | ---: | ---: |
| `mitigation_practices.csv` | 4 | 400 |
| `trade_shock_metrics.csv` | 6 | 600 |
| `electricity_carbon_scenarios.csv` | 5 | 500 |

Original seeds are backed up under `data/structured/seed_backup/`.
