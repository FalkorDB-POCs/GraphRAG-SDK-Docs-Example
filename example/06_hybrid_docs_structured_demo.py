"""
GraphRAG SDK -- Hybrid PDF + structured CSV demo
================================================
Ingest unstructured PDFs from docs/ and structured CSVs from data/structured/
into one FalkorDB graph, finalize once, then run:

  - optional pure-PDF baseline questions (from 05_docs_pdf_demo.py)
  - 10 hybrid-oriented questions (2 structured-only + 8 hybrid)
  - machine-checkable Cypher gold assertions over typed columns

Requires GraphRAG-SDK feat/structured-ingestion (CsvRecordLoader + mapping=).

Prerequisites:
    pip install -e "./GraphRAG-SDK/graphrag_sdk[pdf,litellm]"  # feat/structured-ingestion
    docker run -p 6379:6379 falkordb/falkordb
    export OPENAI_API_KEY="sk-..."

Example:
    python3 example/06_hybrid_docs_structured_demo.py --reset \\
      --output-json docs_hybrid_demo_results.json
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from graphrag_sdk import (
    Column,
    ConnectionConfig,
    GraphRAG,
    LiteLLM,
    LiteLLMEmbedder,
    Table,
)
from graphrag_sdk.ingestion.chunking_strategies.fixed_size import FixedSizeChunking

# Pure-PDF baseline (same list as example/05_docs_pdf_demo.py)
PDF_BASELINE_QUESTIONS: list[str] = [
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

# +10 questions focused on structured / hybrid proof
HYBRID_QUESTIONS: list[dict[str, Any]] = [
    {
        "id": "H1",
        "bucket": "structured",
        "expects_sources": ["csv"],
        "question": (
            "Across all rice mitigation practices in the structured table, what is the "
            "average maximum CH4-reduction percentage?"
        ),
    },
    {
        "id": "H2",
        "bucket": "structured",
        "expects_sources": ["csv"],
        "question": (
            "In the 2025 electricity scenarios table, which country has the larger "
            "expenditure_reduction_pct, Austria or Germany, and by how many percentage points?"
        ),
    },
    {
        "id": "H3",
        "bucket": "hybrid",
        "expects_sources": ["pdf", "csv"],
        "question": (
            "The carbon-farming survey discusses Alternate Wetting and Drying (AWD). "
            "Using the structured table, what CH4-reduction range is stored for AWD, and "
            "how does the paper describe the water-management practice?"
        ),
    },
    {
        "id": "H4",
        "bucket": "hybrid",
        "expects_sources": ["pdf", "csv"],
        "question": (
            "For System of Rice Intensification (SRI), what water-saving range does the "
            "structured table record, and what methane-reduction narrative does the survey provide?"
        ),
    },
    {
        "id": "H5",
        "bucket": "hybrid",
        "expects_sources": ["pdf", "csv"],
        "question": (
            "What soil-carbon or residue benefits does the structured table list for "
            "Green Cane Trash Blanketing (GCTB), and how does the sugarcane section of the "
            "survey explain the practice?"
        ),
    },
    {
        "id": "H6",
        "bucket": "hybrid",
        "expects_sources": ["pdf", "csv"],
        "question": (
            "The survey defines MRV for carbon-farming projects. Which mitigation practices "
            "in the structured table are marked with high mrv_intensity, and what are the "
            "three core MRV components in the paper?"
        ),
    },
    {
        "id": "H7",
        "bucket": "hybrid",
        "expects_sources": ["pdf", "csv"],
        "question": (
            "How does the survey distinguish carbon farming from regenerative and organic "
            "farming, and which structured practice rows are tagged to the carbon-farming "
            "source document 2603.20674v1.pdf?"
        ),
    },
    {
        "id": "H8",
        "bucket": "hybrid",
        "expects_sources": ["pdf", "csv"],
        "question": (
            "What policy event does the trade paper use as its liberalization shock, and "
            "what baseline iceberg trade-cost reduction percentage is stored for that event "
            "in the structured metrics table?"
        ),
    },
    {
        "id": "H9",
        "bucket": "hybrid",
        "expects_sources": ["pdf", "csv"],
        "question": (
            "Which firms and years form the trade paper's empirical sample, and which "
            "structured trade metrics are linked to the China WTO accession policy event?"
        ),
    },
    {
        "id": "H10",
        "bucket": "hybrid",
        "expects_sources": ["pdf", "csv"],
        "question": (
            "Why does a hard price threshold create bunching incentives in the electricity "
            "paper? From the structured scenarios table only, report the exact mechanism and "
            "bunching_risk field values for scenario_id SC-HARD-THRESHOLD versus "
            "SC-LINEAR-RAMP (do not invent percentages or euro amounts)."
        ),
    },
    {
        "id": "H11",
        "bucket": "structured",
        "expects_sources": ["csv"],
        "question": (
            "How many MitigationPractice entities are in the graph after structured "
            "ingestion of the scaled mitigation practices table?"
        ),
    },
    {
        "id": "H12",
        "bucket": "structured",
        "expects_sources": ["csv"],
        "question": (
            "Across all TradeMetric rows linked to policy_event 'China WTO accession', "
            "what is the average value_num?"
        ),
    },

]

MITIGATION_PRACTICES = Table(
    "MitigationPractice",
    key="practice_id",
    name="practice_name",
    crop_system="crop_system",
    ch4_reduction_min_pct=Column("ch4_reduction_min_pct", "FLOAT"),
    ch4_reduction_max_pct=Column("ch4_reduction_max_pct", "FLOAT"),
    water_saving_min_pct=Column("water_saving_min_pct", "FLOAT"),
    water_saving_max_pct=Column("water_saving_max_pct", "FLOAT"),
    soil_carbon_benefit="soil_carbon_benefit",
    mrv_intensity="mrv_intensity",
    source_doc="source_doc",
)

TRADE_METRICS = Table(
    "TradeMetric",
    key="metric_id",
    name="metric_name",
    policy_event="policy_event",
    geography="geography",
    value_num=Column("value_num", "FLOAT"),
    value_unit="value_unit",
    sample_note="sample_note",
    source_doc="source_doc",
)

POLICY_SCENARIOS = Table(
    "PolicyScenario",
    key="scenario_id",
    name="scenario_name",
    country="country",
    year=Column("year", "INTEGER"),
    expenditure_reduction_pct=Column("expenditure_reduction_pct", "FLOAT"),
    mechanism="mechanism",
    bunching_risk="bunching_risk",
    source_doc="source_doc",
)


def _repo_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "docs").is_dir() and (parent / "example").is_dir():
            return parent
    return Path.cwd()


def parse_args() -> argparse.Namespace:
    root = _repo_root()
    parser = argparse.ArgumentParser(
        description="Hybrid PDF + structured CSV GraphRAG demo",
    )
    parser.add_argument(
        "--docs-dir",
        type=Path,
        default=root / "docs",
        help="Directory containing PDF files.",
    )
    parser.add_argument(
        "--structured-dir",
        type=Path,
        default=root / "data" / "structured",
        help="Directory containing structured CSV files.",
    )
    parser.add_argument(
        "--bridge-note",
        type=Path,
        default=root / "data" / "bridge" / "entity_bridge_note.txt",
        help="Optional unstructured bridge note with canonical entity names.",
    )
    parser.add_argument(
        "--skip-bridge-note",
        action="store_true",
        help="Do not ingest the bridge note.",
    )
    parser.add_argument(
        "--graph-name",
        type=str,
        default="docs_hybrid_demo",
        help="FalkorDB graph name.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete all data in the selected graph before ingestion.",
    )
    parser.add_argument(
        "--pdf-questions",
        choices=("none", "sample", "all"),
        default="sample",
        help="Pure-PDF baseline questions: none, a short sample, or all 20.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Optional path to write results JSON.",
    )
    parser.add_argument(
        "--return-context",
        action="store_true",
        help="Include retrieved context items for NL answers.",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=os.getenv("FALKORDB_HOST", "localhost"),
        help="FalkorDB host (default localhost).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("FALKORDB_PORT", "6379")),
        help="FalkorDB port (default 6379).",
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
    pdfs = sorted(p for p in docs_dir.glob("*.pdf") if p.is_file())
    if not pdfs:
        print(f"No PDF files found in: {docs_dir}", file=sys.stderr)
        raise SystemExit(2)
    return pdfs


def structured_sources(structured_dir: Path) -> list[tuple[Path, Table]]:
    mapping_by_name = {
        "mitigation_practices.csv": MITIGATION_PRACTICES,
        "trade_shock_metrics.csv": TRADE_METRICS,
        "electricity_carbon_scenarios.csv": POLICY_SCENARIOS,
    }
    if not structured_dir.exists():
        print(f"structured dir does not exist: {structured_dir}", file=sys.stderr)
        raise SystemExit(2)
    sources: list[tuple[Path, Table]] = []
    for name, mapping in mapping_by_name.items():
        path = structured_dir / name
        if not path.is_file():
            print(f"missing structured source: {path}", file=sys.stderr)
            raise SystemExit(2)
        sources.append((path, mapping))
    return sources


def build_rag(graph_name: str, api_key: str, host: str, port: int) -> GraphRAG:
    # text-embedding-3-small defaults to 1536 dims; GraphRAG must match.
    embedding_dimension = 1536
    llm = LiteLLM(
        model="gpt-4o-mini",
        api_key=api_key,
        temperature=0.0,
    )
    embedder = LiteLLMEmbedder(
        model="text-embedding-3-small",
        api_key=api_key,
        dimensions=embedding_dimension,
    )
    return GraphRAG(
        connection=ConnectionConfig(
            host=host,
            port=port,
            graph_name=graph_name,
        ),
        llm=llm,
        embedder=embedder,
        embedding_dimension=embedding_dimension,
        enable_cypher=True,
    )



def _csv_float_values(path: Path, column: str) -> list[float]:
    values: list[float] = []
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            raw = (row.get(column) or "").strip()
            if raw:
                values.append(float(raw))
    return values


def gold_from_csvs(structured_dir: Path) -> dict[str, Any]:
    mitigation = structured_dir / "mitigation_practices.csv"
    scenarios = structured_dir / "electricity_carbon_scenarios.csv"

    # Seed-only rice practices (ignore synthetic rows added for scale tests).
    seed_rice_ids = {"PR-AWD", "PR-SRI"}
    rice_max = []
    with mitigation.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if (row.get("practice_id") or "").strip() in seed_rice_ids:
                raw = (row.get("ch4_reduction_max_pct") or "").strip()
                if raw:
                    rice_max.append(float(raw))
    avg_rice_max = sum(rice_max) / len(rice_max) if rice_max else None

    at = de = None
    with scenarios.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("scenario_id") == "SC-AT-2025":
                at = float(row["expenditure_reduction_pct"])
            if row.get("scenario_id") == "SC-DE-2025":
                de = float(row["expenditure_reduction_pct"])

    return {
        "mitigation_practice_count": sum(1 for _ in mitigation.open()) - 1,
        "avg_rice_ch4_reduction_max_pct": avg_rice_max,
        "austria_expenditure_reduction_pct": at,
        "germany_expenditure_reduction_pct": de,
        "expenditure_gap_at_minus_de": (
            None if at is None or de is None else round(at - de, 6)
        ),
        "trade_metric_count": sum(
            1 for _ in (structured_dir / "trade_shock_metrics.csv").open()
        )
        - 1,
        "policy_scenario_count": sum(1 for _ in scenarios.open()) - 1,
        "iceberg_baseline_pct": _csv_float_values(
            structured_dir / "trade_shock_metrics.csv",
            "value_num",
        )[0]
        if (structured_dir / "trade_shock_metrics.csv").exists()
        else None,
    }


async def ingest_pdfs(
    rag: GraphRAG,
    pdf_paths: list[Path],
) -> tuple[list[Path], list[tuple[Path, str]]]:
    chunker = FixedSizeChunking(chunk_size=1500, chunk_overlap=200)
    succeeded: list[Path] = []
    failed: list[tuple[Path, str]] = []
    for pdf in pdf_paths:
        print(f"\n[ingest:pdf] {pdf.name}")
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


async def ingest_structured(
    rag: GraphRAG,
    sources: list[tuple[Path, Table]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for path, mapping in sources:
        print(f"\n[ingest:csv] {path.name}")
        result = await rag.ingest(str(path), mapping=mapping)
        payload = {
            "file": path.name,
            "repr": str(result),
        }
        for attr in (
            "nodes_created",
            "relationships_created",
            "chunks_indexed",
            "entities_deleted",
            "no_op",
            "records",
            "entities",
            "chunks",
        ):
            if hasattr(result, attr):
                payload[attr] = getattr(result, attr)
        print(f"  {payload}")
        results.append(payload)
    return results


def _serialize_context_items(result: Any) -> list[dict[str, Any]]:
    if getattr(result, "retriever_result", None) is None:
        return []
    return [
        {
            "score": item.score,
            "content": item.content,
            "metadata": item.metadata,
        }
        for item in result.retriever_result.items
    ]


def _classify_context_item(item: dict[str, Any]) -> str:
    """Best-effort source class for a retrieved context item."""
    meta = item.get("metadata") or {}
    section = str(meta.get("section") or meta.get("source") or meta.get("type") or "").lower()
    content = item.get("content") or ""
    content_l = content.lower()
    blob = f"{section}\n{meta}\n{content[:500]}".lower()

    if "cypher" in section or section == "cypher_results" or "## graph query results" in content_l:
        return "cypher"
    if any(k in blob for k in ("practice_id", "metric_id", "scenario_id", "record_key", "kind: record", "kind\": \"record", "structured")):
        return "csv_record"
    if ".pdf" in blob or "2603." in blob:
        return "pdf"
    if "bridge" in blob or "entity_bridge_note" in blob:
        return "bridge_text"
    if "entity" in section or "## key entities" in content_l:
        return "entity"
    if "chunk" in section or "chunk" in blob:
        return "chunk"
    return "other"


def _source_breakdown(context_items: list[dict[str, Any]]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for item in context_items:
        kind = _classify_context_item(item)
        counts[kind] = counts.get(kind, 0) + 1
    classes = sorted(counts)
    has_structured = any(k in counts for k in ("cypher", "csv_record"))
    has_unstructured = any(k in counts for k in ("pdf", "chunk", "bridge_text", "entity"))
    return {
        "counts": counts,
        "classes": classes,
        "has_structured_signal": has_structured,
        "has_unstructured_signal": has_unstructured,
        "hybrid_context": has_structured and has_unstructured,
    }


def _extract_timing_from_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    """Pull any vendor-provided timing fields if present."""
    md = metadata or {}
    found: dict[str, Any] = {}
    for key, value in md.items():
        lk = str(key).lower()
        if any(tok in lk for tok in ("latency", "duration", "elapsed", "time_ms", "timing")):
            found[key] = value
    # nested common shapes
    for nest_key in ("timings", "timing", "metrics", "perf"):
        nest = md.get(nest_key)
        if isinstance(nest, dict):
            found[nest_key] = nest
    return found


async def _timed_completion(
    rag: GraphRAG,
    question: str,
    *,
    return_context: bool,
) -> tuple[Any, dict[str, Any]]:
    """Run completion while measuring graph/retrieval vs LLM wall times.

    GraphRAG.completion() does retrieve then LLM. We wrap those methods for
    one call so evaluation can report both sides without double-billing the
    question to the model.
    """
    timing: dict[str, Any] = {
        "total_s": None,
        "graph_retrieval_s": None,
        "llm_s": None,
        "other_s": None,
        "retrieve_calls": 0,
        "llm_calls": 0,
    }

    orig_retrieve = rag.retrieve
    llm = rag.llm
    # LiteLLM / providers expose slightly different async entrypoints.
    llm_attr = None
    for name in ("ainvoke_messages", "ainvoke", "complete", "achat"):
        if hasattr(llm, name) and callable(getattr(llm, name)):
            llm_attr = name
            break
    orig_llm = getattr(llm, llm_attr) if llm_attr else None

    async def retrieve_wrap(*args: Any, **kwargs: Any) -> Any:
        timing["retrieve_calls"] += 1
        t0 = time.perf_counter()
        try:
            return await orig_retrieve(*args, **kwargs)
        finally:
            dt = time.perf_counter() - t0
            timing["graph_retrieval_s"] = (timing["graph_retrieval_s"] or 0.0) + dt

    async def llm_wrap(*args: Any, **kwargs: Any) -> Any:
        timing["llm_calls"] += 1
        t0 = time.perf_counter()
        try:
            return await orig_llm(*args, **kwargs)
        finally:
            dt = time.perf_counter() - t0
            timing["llm_s"] = (timing["llm_s"] or 0.0) + dt

    rag.retrieve = retrieve_wrap  # type: ignore[method-assign]
    if orig_llm is not None and llm_attr is not None:
        setattr(llm, llm_attr, llm_wrap)

    t0 = time.perf_counter()
    try:
        result = await rag.completion(question, return_context=return_context)
    finally:
        timing["total_s"] = time.perf_counter() - t0
        rag.retrieve = orig_retrieve  # type: ignore[method-assign]
        if orig_llm is not None and llm_attr is not None:
            setattr(llm, llm_attr, orig_llm)

    g = timing["graph_retrieval_s"]
    l = timing["llm_s"]
    if timing["total_s"] is not None and g is not None and l is not None:
        timing["other_s"] = max(0.0, timing["total_s"] - g - l)
    # round for readability
    for k in ("total_s", "graph_retrieval_s", "llm_s", "other_s"):
        if isinstance(timing.get(k), float):
            timing[k] = round(timing[k], 4)
    timing["llm_method_wrapped"] = llm_attr
    return result, timing


async def run_nl_questions(
    rag: GraphRAG,
    questions: list[dict[str, Any]],
    *,
    return_context: bool,
) -> list[dict[str, Any]]:
    outputs: list[dict[str, Any]] = []
    print(f"\n[query:nl] Running {len(questions)} questions...")
    for idx, item in enumerate(questions, start=1):
        question = item["question"]
        label = item.get("id", f"Q{idx}")
        print(f"\n{label} [{item.get('bucket', 'pdf')}]: {question}")
        try:
            result, timing = await _timed_completion(
                rag,
                question,
                return_context=return_context,
            )
            print(f"A: {result.answer}")
            print(
                "  timing: total={total}s graph/retrieval={graph}s llm={llm}s "
                "other={other}s".format(
                    total=timing.get("total_s"),
                    graph=timing.get("graph_retrieval_s"),
                    llm=timing.get("llm_s"),
                    other=timing.get("other_s"),
                )
            )
            row: dict[str, Any] = {
                "id": label,
                "bucket": item.get("bucket", "pdf"),
                "expects_sources": item.get("expects_sources", ["pdf"]),
                "question": question,
                "answer": result.answer,
                "metadata": result.metadata,
                "timing": {
                    **timing,
                    "sdk_metadata_timing": _extract_timing_from_metadata(
                        result.metadata if isinstance(result.metadata, dict) else {}
                    ),
                },
            }
            ctx_items = _serialize_context_items(result) if return_context else []
            if return_context:
                row["retrieved_context"] = ctx_items
                print(f"  context_items={len(ctx_items)}")
            # Always compute source breakdown when context is available.
            if ctx_items:
                breakdown = _source_breakdown(ctx_items)
            else:
                # Even without stored context, section hints may exist in metadata.
                breakdown = _source_breakdown([])
            row["source_breakdown"] = breakdown
            print(
                "  sources: {counts} hybrid_context={hybrid}".format(
                    counts=breakdown.get("counts"),
                    hybrid=breakdown.get("hybrid_context"),
                )
            )
            outputs.append(row)
        except Exception as exc:  # noqa: BLE001
            message = str(exc)
            print(f"A: ERROR - {message}", file=sys.stderr)
            outputs.append(
                {
                    "id": label,
                    "bucket": item.get("bucket", "pdf"),
                    "expects_sources": item.get("expects_sources", ["pdf"]),
                    "question": question,
                    "error": message,
                }
            )
    return outputs



def _approx_equal(actual: Any, expected: float, tol: float = 1e-6) -> bool:
    try:
        return abs(float(actual) - float(expected)) <= tol
    except (TypeError, ValueError):
        return False


async def run_cypher_assertions(
    rag: GraphRAG,
    gold: dict[str, Any],
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []

    async def run_check(
        name: str,
        cypher: str,
        *,
        expect: Any = None,
        comparator: str = "eq",
    ) -> None:
        print(f"\n[query:cypher] {name}")
        t0 = time.perf_counter()
        try:
            rows = await rag.query(cypher)
            elapsed_s = round(time.perf_counter() - t0, 4)
            actual = rows[0][0] if rows and rows[0] else None
            # multi-column expectations use full first row
            if comparator == "row_eq":
                actual = list(rows[0]) if rows and rows[0] is not None else None
                passed = actual == expect
            elif comparator == "gte":
                passed = actual is not None and float(actual) >= float(expect)
            elif comparator == "approx":
                passed = _approx_equal(actual, expect)
            elif comparator == "gt":
                passed = actual is not None and float(actual) > float(expect)
            else:
                passed = actual == expect
            print(
                f"  actual={actual!r} expected={expect!r} pass={passed} "
                f"graph_s={elapsed_s}"
            )
            checks.append(
                {
                    "name": name,
                    "cypher": cypher,
                    "actual": actual,
                    "expected": expect,
                    "pass": passed,
                    "rows": rows,
                    "timing": {"graph_s": elapsed_s},
                }
            )
        except Exception as exc:  # noqa: BLE001
            elapsed_s = round(time.perf_counter() - t0, 4)
            print(f"  ERROR: {exc}", file=sys.stderr)
            checks.append(
                {
                    "name": name,
                    "cypher": cypher,
                    "error": str(exc),
                    "pass": False,
                    "timing": {"graph_s": elapsed_s},
                }
            )

    await run_check(
        "mitigation_practice_count",
        "MATCH (n:MitigationPractice) RETURN count(n)",
        expect=gold["mitigation_practice_count"],
        comparator="gte",
    )
    await run_check(
        "avg_rice_ch4_reduction_max_pct",
        (
            "MATCH (n:MitigationPractice) "
            "WHERE n.practice_id IN ['PR-AWD', 'PR-SRI'] "
            "AND n.ch4_reduction_max_pct IS NOT NULL "
            "RETURN avg(n.ch4_reduction_max_pct)"
        ),
        expect=gold["avg_rice_ch4_reduction_max_pct"],
        comparator="approx",
    )
    await run_check(
        "austria_vs_germany_gap",
        (
            "MATCH (a:PolicyScenario {scenario_id: 'SC-AT-2025'}), "
            "(d:PolicyScenario {scenario_id: 'SC-DE-2025'}) "
            "RETURN a.expenditure_reduction_pct - d.expenditure_reduction_pct"
        ),
        expect=gold["expenditure_gap_at_minus_de"],
        comparator="approx",
    )
    await run_check(
        "iceberg_baseline_value",
        (
            "MATCH (t:TradeMetric {metric_id: 'TM-ICEBERG'}) "
            "RETURN t.value_num"
        ),
        expect=gold["iceberg_baseline_pct"],
        comparator="approx",
    )

    # Scale checks: table cardinality after 100x expansion.
    await run_check(
        "trade_metric_count",
        "MATCH (n:TradeMetric) RETURN count(n)",
        expect=gold["trade_metric_count"],
        comparator="gte",
    )
    await run_check(
        "policy_scenario_count",
        "MATCH (n:PolicyScenario) RETURN count(n)",
        expect=gold["policy_scenario_count"],
        comparator="gte",
    )
    await run_check(
        "mitigation_record_chunks",
        (
            "MATCH (c:Chunk) "
            "WHERE c.kind = 'record' OR c.record_key IS NOT NULL "
            "RETURN count(c)"
        ),
        expect=gold["mitigation_practice_count"],  # lower bound; total records >> this
        comparator="gte",
    )

    # H10 categorical gold from structured table (anti-hallucination).
    await run_check(
        "h10_hard_vs_ramp_mechanism_risk",
        (
            "MATCH (h:PolicyScenario {scenario_id: 'SC-HARD-THRESHOLD'}), "
            "(r:PolicyScenario {scenario_id: 'SC-LINEAR-RAMP'}) "
            "RETURN h.mechanism, h.bunching_risk, r.mechanism, r.bunching_risk"
        ),
        expect=["hard_threshold", "high", "linear_ramp", "low"],
        comparator="row_eq",
    )

    # Provenance / merge signals.
    await run_check(
        "structured_entity_with_mention_provenance",
        (
            "MATCH (e:__Entity__)-[:MENTIONED_IN]->(c:Chunk) "
            "WHERE e.practice_id IS NOT NULL OR e.metric_id IS NOT NULL "
            "OR e.scenario_id IS NOT NULL "
            "RETURN count(DISTINCT e)"
        ),
        expect=0,
        comparator="gte",
    )
    await run_check(
        "entities_with_alias_ids",
        (
            "MATCH (e:__Entity__) "
            "WHERE e.alias_ids IS NOT NULL AND size(e.alias_ids) > 0 "
            "RETURN count(e)"
        ),
        expect=0,
        comparator="gte",
    )

    return checks


async def diagnose_entity_merge(rag: GraphRAG) -> dict[str, Any]:
    """Inspect why finalize() may report entities_deduplicated=0."""
    print("\n[diagnose] entity merge / alias bridge")
    diag: dict[str, Any] = {"queries": []}

    async def q(name: str, cypher: str) -> Any:
        t0 = time.perf_counter()
        try:
            rows = await rag.query(cypher)
            elapsed = round(time.perf_counter() - t0, 4)
            diag["queries"].append(
                {
                    "name": name,
                    "cypher": cypher,
                    "rows": rows,
                    "timing": {"graph_s": elapsed},
                }
            )
            print(f"  {name}: {rows} ({elapsed}s)")
            return rows
        except Exception as exc:  # noqa: BLE001
            elapsed = round(time.perf_counter() - t0, 4)
            diag["queries"].append(
                {
                    "name": name,
                    "cypher": cypher,
                    "error": str(exc),
                    "timing": {"graph_s": elapsed},
                }
            )
            print(f"  {name}: ERROR {exc}")
            return None

    await q(
        "total_entities",
        "MATCH (e:__Entity__) RETURN count(e)",
    )
    await q(
        "structured_entities",
        (
            "MATCH (e:__Entity__) "
            "WHERE e.practice_id IS NOT NULL OR e.metric_id IS NOT NULL "
            "OR e.scenario_id IS NOT NULL RETURN count(e)"
        ),
    )
    await q(
        "alias_id_entities",
        (
            "MATCH (e:__Entity__) WHERE e.alias_ids IS NOT NULL "
            "AND size(e.alias_ids) > 0 RETURN count(e)"
        ),
    )
    await q(
        "sample_structured_names",
        (
            "MATCH (e:__Entity__) "
            "WHERE e.practice_id IS NOT NULL OR e.metric_id IS NOT NULL "
            "OR e.scenario_id IS NOT NULL "
            "RETURN e.name, e.id, e.alias_ids, labels(e) "
            "ORDER BY e.name LIMIT 20"
        ),
    )
    await q(
        "name_collisions_multi_id",
        (
            "MATCH (e:__Entity__) "
            "WHERE e.name IS NOT NULL "
            "WITH toLower(e.name) AS n, collect(DISTINCT e.id) AS ids, count(*) AS c "
            "WHERE c > 1 "
            "RETURN n, c, ids ORDER BY c DESC LIMIT 20"
        ),
    )
    # Overlap between structured display names and any entity names.
    await q(
        "structured_name_matches_other_entities",
        (
            "MATCH (s:__Entity__) "
            "WHERE s.practice_id IS NOT NULL OR s.metric_id IS NOT NULL "
            "OR s.scenario_id IS NOT NULL "
            "WITH collect(DISTINCT toLower(s.name)) AS snames "
            "MATCH (e:__Entity__) "
            "WHERE e.practice_id IS NULL AND e.metric_id IS NULL "
            "AND e.scenario_id IS NULL AND e.name IS NOT NULL "
            "AND toLower(e.name) IN snames "
            "RETURN e.name, e.id, labels(e) LIMIT 30"
        ),
    )
    await q(
        "bridge_document_present",
        (
            "MATCH (d:Document) "
            "WHERE d.id CONTAINS 'entity_bridge_note' OR d.path CONTAINS 'entity_bridge_note' "
            "OR d.uid CONTAINS 'entity_bridge_note' "
            "RETURN d.id, d.path, d.uid LIMIT 5"
        ),
    )
    return diag


def summarize_question_timings(rows: list[dict[str, Any]]) -> dict[str, Any]:
    totals = []
    graphs = []
    llms = []
    for row in rows:
        t = row.get("timing") or {}
        if isinstance(t.get("total_s"), (int, float)):
            totals.append(float(t["total_s"]))
        if isinstance(t.get("graph_retrieval_s"), (int, float)):
            graphs.append(float(t["graph_retrieval_s"]))
        if isinstance(t.get("llm_s"), (int, float)):
            llms.append(float(t["llm_s"]))

    def _agg(vals: list[float]) -> dict[str, Any]:
        if not vals:
            return {"count": 0}
        return {
            "count": len(vals),
            "total_s": round(sum(vals), 4),
            "avg_s": round(sum(vals) / len(vals), 4),
            "min_s": round(min(vals), 4),
            "max_s": round(max(vals), 4),
        }

    return {
        "wall_total": _agg(totals),
        "graph_retrieval": _agg(graphs),
        "llm": _agg(llms),
    }



def select_pdf_questions(mode: str) -> list[dict[str, Any]]:
    if mode == "none":
        return []
    if mode == "all":
        chosen = PDF_BASELINE_QUESTIONS
    else:
        # Representative sample across the three papers.
        chosen = [
            PDF_BASELINE_QUESTIONS[0],
            PDF_BASELINE_QUESTIONS[1],
            PDF_BASELINE_QUESTIONS[10],
            PDF_BASELINE_QUESTIONS[12],
            PDF_BASELINE_QUESTIONS[17],
            PDF_BASELINE_QUESTIONS[18],
        ]
    return [
        {
            "id": f"P{i}",
            "bucket": "pdf",
            "expects_sources": ["pdf"],
            "question": q,
        }
        for i, q in enumerate(chosen, start=1)
    ]


def write_output_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[output] wrote results to {path}")


def _finalize_to_dict(finalize_result: Any) -> dict[str, Any]:
    if finalize_result is None:
        return {}
    if isinstance(finalize_result, dict):
        return finalize_result
    out: dict[str, Any] = {}
    for attr in (
        "null_stubs_removed",
        "entities_deduplicated",
        "entities_embedded",
        "relationships_embedded",
        "indexes",
    ):
        if hasattr(finalize_result, attr):
            out[attr] = getattr(finalize_result, attr)
    return out or {"repr": str(finalize_result)}


async def ontology_snapshot(rag: GraphRAG) -> list[dict[str, Any]]:
    try:
        ontology = await rag.get_ontology()
    except Exception as exc:  # noqa: BLE001
        return [{"error": str(exc)}]
    snap: list[dict[str, Any]] = []
    for entity in getattr(ontology, "entities", []) or []:
        props = []
        for prop in getattr(entity, "properties", []) or []:
            props.append(
                {
                    "name": getattr(prop, "name", None),
                    "type": getattr(prop, "type", None),
                }
            )
        if props:
            snap.append({"label": getattr(entity, "label", None), "properties": props})
    return snap


async def main_async(args: argparse.Namespace) -> int:
    api_key = require_openai_api_key()
    pdf_paths = discover_pdfs(args.docs_dir)
    csv_sources = structured_sources(args.structured_dir)
    gold = gold_from_csvs(args.structured_dir)

    print(f"[setup] docs_dir={args.docs_dir}")
    print(f"[setup] structured_dir={args.structured_dir}")
    print(f"[setup] pdfs={len(pdf_paths)} csvs={len(csv_sources)}")
    print(f"[setup] graph_name={args.graph_name} host={args.host}:{args.port}")
    print(f"[setup] enable_cypher=True pdf_questions={args.pdf_questions}")

    rag = build_rag(
        graph_name=args.graph_name,
        api_key=api_key,
        host=args.host,
        port=args.port,
    )

    if args.reset:
        print("\n[reset] deleting existing graph data...")
        try:
            # Public API on feat/structured-ingestion (not rag.graph_store).
            await rag.delete_all()
            print("[reset] done")
        except Exception as exc:  # noqa: BLE001
            print(f"[reset] warning: {exc}", file=sys.stderr)

    pdf_ok, pdf_failed = await ingest_pdfs(rag, pdf_paths)
    if not pdf_ok:
        print("\nNo PDFs were ingested successfully. Aborting.", file=sys.stderr)
        return 1

    if not args.skip_bridge_note and args.bridge_note.is_file():
        print(f"\n[ingest:text] bridge note {args.bridge_note.name}")
        try:
            text = args.bridge_note.read_text(encoding="utf-8")
            bridge_result = await rag.ingest(
                text=text,
                document_id=str(args.bridge_note),
            )
            print(
                f"  nodes={bridge_result.nodes_created} "
                f"relationships={bridge_result.relationships_created} "
                f"chunks={bridge_result.chunks_indexed}"
            )
            bridge_status = {
                "file": args.bridge_note.name,
                "nodes_created": bridge_result.nodes_created,
                "relationships_created": bridge_result.relationships_created,
                "chunks_indexed": bridge_result.chunks_indexed,
            }
        except Exception as exc:  # noqa: BLE001
            print(f"  bridge note failed: {exc}", file=sys.stderr)
            bridge_status = {"file": args.bridge_note.name, "error": str(exc)}
    else:
        bridge_status = {"skipped": True}

    structured_results = await ingest_structured(rag, csv_sources)

    print("\n[finalize] running once after all ingestions...")
    try:
        finalize_result = await rag.finalize()
        finalize_payload = _finalize_to_dict(finalize_result)
        print(f"[finalize] {finalize_payload}")
    except Exception as exc:  # noqa: BLE001
        print(f"[finalize] failed: {exc}", file=sys.stderr)
        return 1

    merge_diagnostics = await diagnose_entity_merge(rag)

    ontology = await ontology_snapshot(rag)
    if ontology:
        print("\n[ontology] declared properties (structured labels expected):")
        for ent in ontology:
            if ent.get("error"):
                print(f"  error: {ent['error']}")
                continue
            props = ", ".join(
                f"{p.get('name')}:{p.get('type')}" for p in ent.get("properties", [])
            )
            print(f"  {ent.get('label')}: {props}")

    pdf_q = select_pdf_questions(args.pdf_questions)
    pdf_answers = await run_nl_questions(
        rag,
        pdf_q,
        return_context=args.return_context,
    )
    hybrid_answers = await run_nl_questions(
        rag,
        HYBRID_QUESTIONS,
        return_context=args.return_context,
    )
    cypher_checks = await run_cypher_assertions(rag, gold)

    cypher_pass = sum(1 for c in cypher_checks if c.get("pass"))
    hybrid_errors = sum(1 for item in hybrid_answers if "error" in item)
    pdf_errors = sum(1 for item in pdf_answers if "error" in item)
    pdf_timing = summarize_question_timings(pdf_answers)
    hybrid_timing = summarize_question_timings(hybrid_answers)
    all_timing = summarize_question_timings(pdf_answers + hybrid_answers)
    cypher_graph_vals = [
        float((c.get("timing") or {}).get("graph_s"))
        for c in cypher_checks
        if isinstance((c.get("timing") or {}).get("graph_s"), (int, float))
    ]
    cypher_timing = {
        "count": len(cypher_graph_vals),
        "total_s": round(sum(cypher_graph_vals), 4) if cypher_graph_vals else 0.0,
        "avg_s": round(sum(cypher_graph_vals) / len(cypher_graph_vals), 4)
        if cypher_graph_vals
        else None,
    }

    print(
        f"\n[summary] pdf_questions_ok={len(pdf_answers) - pdf_errors}/{len(pdf_answers)} "
        f"hybrid_questions_ok={len(hybrid_answers) - hybrid_errors}/{len(hybrid_answers)} "
        f"cypher_pass={cypher_pass}/{len(cypher_checks)} "
        f"finalize_dedup={finalize_payload.get('entities_deduplicated')}"
    )
    print(
        "[summary:timing] nl_total_avg={nl_avg}s "
        "nl_graph_avg={g_avg}s nl_llm_avg={l_avg}s "
        "cypher_graph_total={cg}s".format(
            nl_avg=(all_timing.get("wall_total") or {}).get("avg_s"),
            g_avg=(all_timing.get("graph_retrieval") or {}).get("avg_s"),
            l_avg=(all_timing.get("llm") or {}).get("avg_s"),
            cg=cypher_timing.get("total_s"),
        )
    )

    if args.output_json is not None:
        payload = {
            "graph_name": args.graph_name,
            "docs_dir": str(args.docs_dir),
            "structured_dir": str(args.structured_dir),
            "gold": gold,
            "ingestion": {
                "pdfs_succeeded": [p.name for p in pdf_ok],
                "pdfs_failed": [
                    {"file": p.name, "error": err} for p, err in pdf_failed
                ],
                "bridge_note": bridge_status,
                "structured": structured_results,
            },
            "finalize": finalize_payload,
            "merge_diagnostics": merge_diagnostics,
            "ontology": ontology,
            "pdf_baseline_questions": pdf_answers,
            "hybrid_questions": hybrid_answers,
            "cypher_assertions": cypher_checks,
            "evaluation_timing": {
                "pdf_baseline_questions": pdf_timing,
                "hybrid_questions": hybrid_timing,
                "all_nl_questions": all_timing,
                "cypher_assertions": cypher_timing,
            },
        }
        write_output_json(args.output_json, payload)

    # Non-zero if core structured gold checks failed hard.
    hard_names = {
        "mitigation_practice_count",
        "avg_rice_ch4_reduction_max_pct",
        "austria_vs_germany_gap",
        "iceberg_baseline_value",
        "h10_hard_vs_ramp_mechanism_risk",
    }
    hard_fail = any(
        (not c.get("pass")) and c.get("name") in hard_names for c in cypher_checks
    )
    if hard_fail or not pdf_ok:
        return 1
    return 0


def main() -> None:
    args = parse_args()
    exit_code = asyncio.run(main_async(args))
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
