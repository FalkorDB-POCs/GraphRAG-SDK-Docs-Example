# GraphRAG PDF Demo (docs + example only)
This repository is intentionally minimal and includes only:
- `docs/` (PDF source documents)
- `example/05_docs_pdf_demo.py` (demo runner)

`GraphRAG-SDK` is **not** vendored here and should be downloaded separately from the `staging` branch:
https://github.com/FalkorDB/GraphRAG-SDK/tree/staging

## 1) Clone GraphRAG-SDK from `staging`
Run this from the root of this repo:

```zsh
git clone --depth 1 --branch staging https://github.com/FalkorDB/GraphRAG-SDK.git
```

This creates:

```text
.
├── docs/
├── example/
│   └── 05_docs_pdf_demo.py
└── GraphRAG-SDK/
```

## 2) Create a Python environment and install SDK dependencies
```zsh
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e "./GraphRAG-SDK/graphrag_sdk[pdf,litellm]"
```

If you cloned `GraphRAG-SDK` elsewhere, use its absolute path:

```zsh
pip install -e "/absolute/path/to/GraphRAG-SDK/graphrag_sdk[pdf,litellm]"
```

## 3) Start FalkorDB
```zsh
docker run --name falkordb -p 6379:6379 falkordb/falkordb
```

If the container already exists:

```zsh
docker start falkordb
```

## 4) Set your OpenAI API key
```zsh
export OPENAI_API_KEY="<your_openai_key>"
```

## 5) Run the demo against `docs/`
```zsh
python3 example/05_docs_pdf_demo.py \
  --docs-dir docs \
  --graph-name docs_pdf_demo \
  --reset \
  --output-json docs_pdf_demo_results.json
```

### Optional: include retrieved context in output
```zsh
python3 example/05_docs_pdf_demo.py \
  --docs-dir docs \
  --graph-name docs_pdf_demo \
  --return-context \
  --output-json docs_pdf_demo_results_with_context.json
```

## What the script does
1. Discovers all `*.pdf` files in `docs/`
2. Ingests them into FalkorDB with GraphRAG
3. Runs `finalize()` once after ingestion
4. Executes a fixed set of 20 questions
5. Writes answers to the JSON output file

## Troubleshooting
- `ModuleNotFoundError: graphrag_sdk`  
  Re-run the editable install command in step 2.
- `pypdf is required for PDF loading`  
  Ensure you installed the `pdf` extra: `...[pdf,litellm]`.
- `Missing OPENAI_API_KEY`  
  Export the key in your current shell session.
- Connection errors to FalkorDB  
  Confirm the container is running and port `6379` is available.
