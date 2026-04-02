"""
GraphRAG SDK v2 -- Multi-PDF docs/ Demo
========================================
Ingest all PDF files from a docs/ folder, finalize once, then run a fixed
20-question benchmark over the combined graph.
Run from repo root (recommended):
    /where-you-cloned/GraphRAG-SDK-Docs-Example-repo/
Prerequisites:
    pip install -e "./GraphRAG-SDK/graphrag_sdk[pdf,litellm]"
    docker run -p 6379:6379 falkordb/falkordb
    export OPENAI_API_KEY="<your_openai_key>"

Usage with docs/ folder:
    python3 example/05_docs_pdf_demo.py --docs-dir docs --graph-name docs_pdf_demo --reset

Usage with JSON output:
    python3 example/05_docs_pdf_demo.py \
      --docs-dir docs \
      --graph-name docs_pdf_demo \
      --output-json docs_pdf_demo_results.json

Usage with retrieval context debugging:
    python3 example/05_docs_pdf_demo.py \
      --docs-dir docs \
      --graph-name docs_pdf_demo \
      --return-context \
      --output-json docs_pdf_demo_results_with_context.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

from graphrag_sdk import ConnectionConfig, GraphRAG, LiteLLM, LiteLLMEmbedder
from graphrag_sdk.ingestion.chunking_strategies.fixed_size import FixedSizeChunking

QUESTIONS: list[str] = [
    "What policy event is used as the trade-liberalization shock in the trade paper?",
    "By roughly what percentage does China's WTO accession reduce iceberg trade cost in "
    "the baseline estimate?",
    "Which firms and years are used for the empirical sample in that study?",
    "Through which three channels does export promotion increase product innovation after "
    "trade liberalization?",
    "What are the estimated CES preference parameters for domestic and export markets?",
    "What is the estimated firm exit probability reported in the sample?",
    "What do the estimates imply about entry costs versus fixed costs for export and "
    "innovation decisions?",
    "What robustness test excludes processing-trade firms, and does the main cost-pattern "
    "result remain?",
    "According to the carbon-farming survey, what share of India's transport emissions could "
    "be offset if all cropland adopted carbon farming?",
    "What three climate-mitigation pathways are identified for agricultural land management "
    "in the survey?",
    "What are the three core components of MRV in carbon-farming projects?",
    "How does the survey distinguish carbon farming from regenerative and organic farming?",
    "What methane-reduction range is reported for Alternate Wetting and Drying (AWD) in "
    "rice cultivation?",
    "What water-saving and methane-reduction ranges are reported for the System of Rice "
    "Intensification (SRI)?",
    "In sugarcane systems, what is Green Cane Trash Blanketing and what are its "
    "carbon-related benefits?",
    "What are the two major categories of carbon markets, and how are they different?",
    "What settlement rule is proposed in the electricity paper to reduce consumer exposure "
    "to carbon-cost pass-through?",
    "In the 2025 baseline simulation, what expenditure reductions are estimated for Austria "
    "and Germany?",
    "Why does a hard price threshold create bunching incentives around the activation point?",
    "How does a linear phase-in (ramp) of the deduction mitigate threshold bunching "
    "incentives?",
]


def _default_docs_dir() -> Path:
    # Find the nearest ancestor that contains a docs/ directory.
    # Works for both:
    # - example/05_docs_pdf_demo.py
    # - GraphRAG-SDK/graphrag_sdk/examples/05_docs_pdf_demo.py
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "docs"
        if candidate.exists() and candidate.is_dir():
            return candidate
    return Path.cwd() / "docs"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="GraphRAG multi-PDF docs demo")
    parser.add_argument(
        "--docs-dir",
        type=Path,
        default=_default_docs_dir(),
        help="Directory containing PDF files to ingest.",
    )
    parser.add_argument(
        "--graph-name",
        type=str,
        default="docs_pdf_demo",
        help="FalkorDB graph name to use for this demo.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete all data in the selected graph before ingestion.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Optional path to write query results as JSON.",
    )
    parser.add_argument(
        "--return-context",
        action="store_true",
        help="Return and store retriever context items for each answer.",
    )
    return parser.parse_args()


def require_openai_api_key() -> str:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        print(
            "Missing OPENAI_API_KEY. Export it before running this demo.",
            file=sys.stderr,
        )
        raise SystemExit(2)
    return api_key


def discover_pdfs(docs_dir: Path) -> list[Path]:
    if not docs_dir.exists() or not docs_dir.is_dir():
        print(f"docs directory does not exist: {docs_dir}", file=sys.stderr)
        raise SystemExit(2)
    pdfs = sorted([p for p in docs_dir.glob("*.pdf") if p.is_file()])
    if not pdfs:
        print(f"No PDF files found in: {docs_dir}", file=sys.stderr)
        raise SystemExit(2)
    return pdfs


def build_rag(graph_name: str, api_key: str) -> GraphRAG:
    llm = LiteLLM(
        model="gpt-4o-mini",
        api_key=api_key,
        temperature=0.0,
    )
    embedder = LiteLLMEmbedder(
        model="text-embedding-3-small",
        api_key=api_key,
    )
    return GraphRAG(
        connection=ConnectionConfig(host="localhost", graph_name=graph_name),
        llm=llm,
        embedder=embedder,
    )


async def ingest_pdfs(rag: GraphRAG, pdf_paths: list[Path]) -> tuple[list[Path], list[tuple[Path, str]]]:
    chunker = FixedSizeChunking(chunk_size=1500, chunk_overlap=200)
    succeeded: list[Path] = []
    failed: list[tuple[Path, str]] = []

    for pdf in pdf_paths:
        print(f"\n[ingest] {pdf.name}")
        try:
            result = await rag.ingest(str(pdf), chunker=chunker)
            print(
                "  nodes={nodes} relationships={rels} chunks={chunks}".format(
                    nodes=result.nodes_created,
                    rels=result.relationships_created,
                    chunks=result.chunks_indexed,
                )
            )
            succeeded.append(pdf)
        except Exception as exc:  # noqa: BLE001
            message = str(exc)
            print(f"  failed: {message}", file=sys.stderr)
            failed.append((pdf, message))

    return succeeded, failed


def _serialize_context_items(result: Any) -> list[dict[str, Any]]:
    if result.retriever_result is None:
        return []
    return [
        {
            "score": item.score,
            "content": item.content,
            "metadata": item.metadata,
        }
        for item in result.retriever_result.items
    ]


async def run_questions(rag: GraphRAG, return_context: bool) -> list[dict[str, Any]]:
    outputs: list[dict[str, Any]] = []

    print(f"\n[query] Running {len(QUESTIONS)} questions...")
    for idx, question in enumerate(QUESTIONS, start=1):
        print(f"\nQ{idx}: {question}")
        try:
            result = await rag.query(question, return_context=return_context)
            print(f"A{idx}: {result.answer}")

            item: dict[str, Any] = {
                "index": idx,
                "question": question,
                "answer": result.answer,
                "metadata": result.metadata,
            }
            if return_context:
                context_items = _serialize_context_items(result)
                item["retrieved_context"] = context_items
                print(f"  context_items={len(context_items)}")
            outputs.append(item)
        except Exception as exc:  # noqa: BLE001
            message = str(exc)
            print(f"A{idx}: ERROR - {message}", file=sys.stderr)
            outputs.append(
                {
                    "index": idx,
                    "question": question,
                    "error": message,
                }
            )
    return outputs


def write_output_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[output] wrote results to {path}")


async def main_async(args: argparse.Namespace) -> int:
    api_key = require_openai_api_key()
    pdf_paths = discover_pdfs(args.docs_dir)

    print(f"[setup] docs_dir={args.docs_dir}")
    print(f"[setup] discovered {len(pdf_paths)} PDFs")
    print(f"[setup] graph_name={args.graph_name}")

    rag = build_rag(graph_name=args.graph_name, api_key=api_key)

    if args.reset:
        print("\n[reset] deleting existing graph data...")
        try:
            await rag.graph_store.delete_all()
            print("[reset] done")
        except Exception as exc:  # noqa: BLE001
            print(f"[reset] warning: {exc}", file=sys.stderr)

    succeeded, failed = await ingest_pdfs(rag, pdf_paths)
    if not succeeded:
        print("\nNo PDFs were ingested successfully. Aborting.", file=sys.stderr)
        for path, error in failed:
            print(f"  - {path.name}: {error}", file=sys.stderr)
        return 1

    print("\n[finalize] running once after all ingestions...")
    try:
        finalize_result = await rag.finalize()
        print(
            "[finalize] null_stubs_removed={nulls} deduplicated={dedup} "
            "entities_embedded={entities} relationships_embedded={rels}".format(
                nulls=finalize_result.get("null_stubs_removed", 0),
                dedup=finalize_result.get("entities_deduplicated", 0),
                entities=finalize_result.get("entities_embedded", 0),
                rels=finalize_result.get("relationships_embedded", 0),
            )
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[finalize] failed: {exc}", file=sys.stderr)
        return 1

    answers = await run_questions(rag, return_context=args.return_context)

    if args.output_json is not None:
        payload = {
            "graph_name": args.graph_name,
            "docs_dir": str(args.docs_dir),
            "ingestion": {
                "succeeded": [p.name for p in succeeded],
                "failed": [{"file": p.name, "error": err} for p, err in failed],
            },
            "finalize": finalize_result,
            "questions": answers,
        }
        write_output_json(args.output_json, payload)

    if failed:
        print("\n[summary] some PDFs failed ingestion:")
        for path, error in failed:
            print(f"  - {path.name}: {error}")
    else:
        print("\n[summary] all PDFs ingested successfully")

    question_errors = sum(1 for item in answers if "error" in item)
    print(
        f"[summary] answered={len(answers) - question_errors} failed_questions={question_errors} "
        f"total={len(answers)}"
    )
    return 0


def main() -> None:
    args = parse_args()
    exit_code = asyncio.run(main_async(args))
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
