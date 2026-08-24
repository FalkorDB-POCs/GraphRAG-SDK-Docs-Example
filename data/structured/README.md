# Structured inputs for the hybrid docs demo

These CSVs are ingested with GraphRAG-SDK structured mappings (`Table` and
`Link`) into the **same FalkorDB graph** as the PDFs under `docs/`.

## Design rules

1. **A row joins a document only where a value matches its wording exactly.**
   `practice_name` does for three of four practices. The other two files join
   through a `Link` on a short column (`geography`, `country`) instead — see
   below.
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

The seed rows are the data. An earlier revision expanded each file 100x with
synthetic rows to simulate a large table, and that turned out to cost more than
it demonstrated: with 1,485 invented rows against 6 extracted from prose, the
graph was almost entirely filler, and the padding surfaced in answers. Asked
which practices have high MRV intensity, the demo replied "Synthetic Mitigation
Practice 0396, 0393, 0390...".

Scale is worth measuring, but not in the same run that demonstrates joining a
table to a document. The SDK's own docs carry throughput figures (50,000 rows in
38.5s), and `seed_backup/` still holds these files if a padded copy is ever
wanted again.

## How each file joins the PDFs

Only **exact** name matches join. That is deliberate in the SDK: it never merges
two things that might not be the same. It also means a display name written for
a CSV will not join anything, because the paper does not contain that phrase.

Checked against the full text of all three PDFs:

| File | Names appearing verbatim in a PDF |
| --- | --- |
| `mitigation_practices.csv` | 3 of 4 — `Alternate Wetting and Drying (AWD)`, `Green Cane Trash Blanketing (GCTB)`, `Carbon Farming` |
| `trade_shock_metrics.csv` | 0 of 6 |
| `electricity_carbon_scenarios.csv` | 0 of 5 |

So the practices join on their names, and the other two cannot. What *does*
appear verbatim in the papers is the short values in their other columns:
`China`, `Austria`, `Germany`, `EU`, `rice`, `sugarcane`.

Those are now joined with a `Link`, which turns a column holding another
entity's key into a real edge:

```python
TRADE_METRICS = Table(
    "TradeMetric", key="metric_id", name="metric_name",
    value_num=Column("value_num", "FLOAT"),
    links=[Link("MEASURED_IN", to="Location", by="geography")],
)
```

The paper mentions China, the table links six metrics to China, and a question
can now walk from the paper's prose into the typed numbers.
