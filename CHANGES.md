# What changed, and why

Four changes to the hybrid demo, after running it and finding that its central
claim was not happening. Two of the four are fixes to this repo. One was a bug in
the SDK, now fixed on `feat/structured-ingestion`. Nothing here is a criticism of
the demo's design: the mappings were right, the questions were right, and the
diagnostic you wrote is what made the cause findable.

---

## 1. The two halves were never joining

`finalize()` reported `entities_deduplicated: 0` on every run. The graph looked
fully populated and every question answered plausibly, so nothing pointed at a
problem.

(Your `hybrid_context` instrumentation was *not* the tell, and an earlier draft
of this note said it was. Run with `--return-context`, 11 of 12 questions report
`hybrid_context=True`. Without that flag there are no context items to classify,
so it reads `False` for every question regardless. The instrumentation is fine;
the reading was mine.)

The cause was ingest order, and it was an SDK bug rather than anything in this
repo. Resolution matches on **name and label**, which is what stops "Apple" the
company merging with "Apple" the fruit. But an extractor can only label an entity
with a label the ontology already has. This demo reads the PDFs first, when the
ontology knows only the built-in labels, so "Carbon Farming" was filed as a
`Concept`. The CSV then declared it a `MitigationPractice`. Same name, different
label, so resolution correctly refused to join them — and said nothing.

Your diagnostic caught it exactly:

```
carbon farming  ->  carbon_farming__concept          (from the PDF)
                ->  pr-cf__mitigationpractice        (from the CSV)
```

Measured on your files, changing only the order:

| Order | Entities joined |
| --- | ---: |
| PDFs first (this demo) | 0 |
| CSVs first | 5 |

**Fixed in the SDK, not worked around here.** A declared type now beats a guessed
one: when a name exists under one label a mapping declared and one or more labels
nothing declared, the declared label survives and absorbs the rest, keeping the
document's description. Two *declared* labels sharing a name are still kept
apart, and so is a name under only guessed labels, which is the Apple case.
Anything left unmerged is reported as `FinalizeResult.unmerged_name_collisions`
instead of vanishing into a zero.

So this demo now works in its original order, and `--structured-first` was added
to demonstrate that rather than assert it.

## 2. The joins went to the bridge note, never to a paper

Every join this demo made was CSV to `entity_bridge_note.txt`. Not one reached an
actual PDF. That matters because a customer will not have a note that restates
their column values, so the demo proved the mechanism on a scaffold built to make
it work.

Checked against the full extracted text of all three PDFs:

| File | Real rows | Names appearing verbatim in a paper |
| --- | ---: | ---: |
| `mitigation_practices.csv` | 4 | 3 |
| `trade_shock_metrics.csv` | 6 | **0** |
| `electricity_carbon_scenarios.csv` | 5 | **0** |

`Iceberg trade cost reduction from China WTO accession` is a phrase written for
the CSV, not a phrase in the paper, so no extractor will ever produce it and it
can never join by name.

But the shorter values in the other columns are in the papers: `China`,
`Austria`, `Germany`, `EU`, `rice`, `sugarcane`. So those are now joined with a
`Link`, which turns a column holding another entity's key into a real edge:

```python
TRADE_METRICS = Table(
    "TradeMetric", key="metric_id", name="metric_name",
    value_num=Column("value_num", "FLOAT"),
    ...
    links=[Link("MEASURED_IN", to="Location", by="geography")],
)
```

Result, the first real CSV-to-PDF join this demo has had:

```
joined: China
        ['2603.23825v1.pdf', 'trade_shock_metrics.csv']
```

Six trade metrics now hang off the `China` the paper itself mentions, so a
question can walk from the prose into the typed numbers.

## 3. The synthetic padding was leaking into answers

The 100x expansion was documented as a scale simulation, but it competed with
the thing the demo exists to show: 1,485 invented rows against 6 entities
extracted from prose. Asked which practices have high MRV intensity, the demo
answered:

```
1. Synthetic Mitigation Practice 0396
2. Synthetic Mitigation Practice 0393
3. Synthetic Mitigation Practice 0390
...
```

The CSVs are back to the seed rows, restored from your own `seed_backup/`, which
still holds the padded copies. Scale is worth measuring, just not in the run that
demonstrates joining — the SDK's docs now carry throughput figures (50,000 rows
in 38.5s, memory flat, no model calls on that path).

## 4. Nothing failed when the demo proved nothing

This is the change worth keeping regardless of the rest. The run could report
success with zero joins, because the questions still answered plausibly: the
Cypher rows and the prose chunks reach the model's context separately, so an
answer can look hybrid while the graph is two disconnected halves. `hybrid_context`
does not catch that either — it asks whether both kinds of context arrived, which
is true whether or not the graph connects them.

`assert_halves_are_joined()` now prints every entity reachable from more than one
source and **fails** when there are none:

```
[assert] are the two halves actually joined?
  joined: Alternate Wetting and Drying (AWD)
          ['entity_bridge_note.txt', 'mitigation_practices.csv']
  joined: China
          ['2603.23825v1.pdf', 'trade_shock_metrics.csv']
  ...
  RESULT: 10 entity/entities span more than one source -> PASS
```

---

## Verifying it

Both orders, one PDF (the trade paper), all three CSVs:

```zsh
python3 example/06_hybrid_docs_structured_demo.py \
  --docs-dir docs --structured-dir data/structured \
  --graph-name docs_hybrid_demo --reset --pdf-questions none

python3 example/06_hybrid_docs_structured_demo.py \
  --docs-dir docs --structured-dir data/structured \
  --graph-name docs_hybrid_demo_alt --reset --pdf-questions none \
  --structured-first
```

| | PDFs first | CSVs first |
| --- | ---: | ---: |
| Entities spanning more than one source | 10 PASS | 7 PASS |
| Trade metrics linked to `China` | 6 | 6 |
| Those reachable from the paper's own prose | 6 | 6 |

The totals differ because the extractor's label choices vary between runs; it is
a model, not a function. Both pass, and neither number is "the" number.

## Caveats

- Tested with one PDF to keep cost down — the trade paper, the one whose `Link`
  could be verified against its text. The other two CSVs still join through the
  bridge note rather than their own papers.
- The practices join on their names, which works, but three of four is luck of
  phrasing rather than something the design guarantees.
- Requires `feat/structured-ingestion` at `02b19d8` or later, which is where the
  ordering fix and `Link` live.
