# GraphRAG Docs Demo (PDF + structured tables)

This repository is intentionally small and includes:

- `docs/` — unstructured PDF source documents
- `data/structured/` — CSV tables aligned to PDF entity names
- `data/bridge/` — optional unstructured bridge note with canonical names
- `example/05_docs_pdf_demo.py` — pure PDF baseline (20 questions)
- `example/06_hybrid_docs_structured_demo.py` — hybrid PDF + CSV demo

`GraphRAG-SDK` is **not** vendored here. For the hybrid demo you need the
local **`feat/structured-ingestion`** branch (structured `mapping=` path +
`CsvRecordLoader`). The pure PDF demo can still run on `main`.

## Layout

```text
.
├── docs/
│   ├── 2603.20674v1.pdf          # Carbon farming survey
│   ├── 2603.23825v1.pdf          # China WTO / trade & innovation
│   └── 2603.25874v1.pdf          # Carbon–electricity market design
├── data/
│   ├── structured/
│   │   ├── mitigation_practices.csv
│   │   ├── trade_shock_metrics.csv
│   │   ├── electricity_carbon_scenarios.csv
│   │   └── README.md
│   └── bridge/
│       └── entity_bridge_note.txt
└── example/
    ├── 05_docs_pdf_demo.py
    └── 06_hybrid_docs_structured_demo.py
```

## 1) Install GraphRAG-SDK

### Hybrid demo (required): structured-ingestion branch

```zsh
# if you already have the branch checked out locally:
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e "/path/to/GraphRAG-SDK/graphrag_sdk[pdf,litellm]"
```

Or clone that branch into this repo:

```zsh
git clone --depth 1 --branch feat/structured-ingestion \
  https://github.com/FalkorDB/GraphRAG-SDK.git
pip install -e "./GraphRAG-SDK/graphrag_sdk[pdf,litellm]"
```

### Pure PDF demo only (`main` is enough)

```zsh
git clone --depth 1 --branch main https://github.com/FalkorDB/GraphRAG-SDK.git
pip install -e "./GraphRAG-SDK/graphrag_sdk[pdf,litellm]"
```

## 2) Start FalkorDB

```zsh
docker run --name falkordb -p 6379:6379 falkordb/falkordb
```

If the container already exists:

```zsh
docker start falkordb
```

## 3) Set your OpenAI API key

```zsh
export OPENAI_API_KEY="<your_openai_key>"
```

## 4a) Run the pure PDF baseline

```zsh
python3 example/05_docs_pdf_demo.py \
  --docs-dir docs \
  --graph-name docs_pdf_demo \
  --reset \
  --output-json docs_pdf_demo_results.json
```

## 4b) Run the hybrid PDF + structured demo

This ingests:

1. all PDFs in `docs/`
2. optional bridge note (`data/bridge/entity_bridge_note.txt`)
3. three CSVs via declared `Table` mappings (built-in engine: **CSV only**)
4. `finalize()` once (merges structured + prose entities when names align)
5. optional pure-PDF sample questions
6. **10 hybrid-oriented questions** (2 structured-only + 8 hybrid)
7. Cypher gold assertions over typed columns

```zsh
python3 example/06_hybrid_docs_structured_demo.py \
  --docs-dir docs \
  --structured-dir data/structured \
  --graph-name docs_hybrid_demo \
  --reset \
  --pdf-questions sample \
  --return-context \
  --output-json docs_hybrid_demo_results.json
```

Useful flags:

- `--pdf-questions none|sample|all` — pure-PDF baseline set
- `--skip-bridge-note` — skip the unstructured name-bridge text
- `--host` / `--port` — FalkorDB endpoint (defaults `localhost:6379`)

### Structured engine note

GraphRAG-SDK structured ingestion ships **`CsvRecordLoader` only** (delimiter
sniff: comma, semicolon, tab, pipe). Parquet/SQL/etc. require a custom
`RecordLoaderStrategy`.

### Why the CSVs match the PDFs

Display names reuse paper wording so resolution can bridge halves, e.g.:

- `Alternate Wetting and Drying (AWD)`
- `System of Rice Intensification (SRI)`
- `Green Cane Trash Blanketing (GCTB)`
- `China WTO accession` / iceberg trade-cost metrics
- Austria / Germany 2025 expenditure scenarios, hard threshold, linear ramp

See `data/structured/README.md`.

## Troubleshooting

- `ModuleNotFoundError: graphrag_sdk`  
  Re-run the editable install for the correct branch.
- `Table` / `mapping=` import or ingest errors  
  You are not on `feat/structured-ingestion`.
- `pypdf is required for PDF loading`  
  Install the `pdf` extra: `...[pdf,litellm]`.
- `Missing OPENAI_API_KEY`  
  Export the key in your current shell session.
- Connection errors to FalkorDB  
  Confirm the container is running and port `6379` is available.
- Structured Cypher assertions fail  
  Confirm CSV ingest logs succeeded and `enable_cypher=True` is set (the hybrid
  script sets this automatically).
