import argparse
import asyncio
import os
from pathlib import Path
from typing import Any

from cap.federated.models import FederatedExecutionResult, FederatedQuery
from cap.federated.service import execute_federated_query
from cap.services.agentic.tools import format_execution_context
from cap.services.telegram_chart_renderer import render_telegram_image
from cap.services.vega.facade import VegaConverter
from cap.util.query_file_parser import QueryFileParser


class FakeDb:
    def add(self, obj: object) -> None:
        _ = obj

    def commit(self) -> None:
        return


class FakeUser:
    user_id = 1


def extract_query_blocks(content: str) -> list[FederatedQuery]:
    queries: list[FederatedQuery] = []

    raw_queries = QueryFileParser.parse(content)
    for nl_query, payload in raw_queries:
        sparql = str(payload.get("sparql", "")).strip()
        sql = str(payload.get("sql", "")).strip()
        visualization_type = payload.get("visualization_type") or "text"
        explanation = str(payload.get("explanation", "")).strip()
        source = str(payload.get("source", "")).strip()
        if sparql or sql:
            queries.append(
                FederatedQuery(
                    sparql=sparql,
                    sql=sql,
                    visualization_type=visualization_type,
                    explanation=explanation,
                    source=source,
                    language="en",
                    post_processing=None,
                    nl_query=nl_query,
                )
            )

    return queries


def count_rows(sparql_results: dict[str, Any]) -> int:
    bindings = sparql_results.get("results", {}).get("bindings", [])
    if bindings:
        return len(bindings)

    if sparql_results.get("boolean") is not None:
        return 1

    return 0


def normalize_render_type(render_type: str) -> str:
    if render_type in VegaConverter.known_types:
        return render_type

    return "text"


async def render_query_results(
    *,
    federated_query: FederatedQuery,
    index: int,
    total: int,
) -> None:
    print(f"\n[{index}/{total}] Executing:")
    print(federated_query.nl_query)

    result: FederatedExecutionResult = await execute_federated_query(federated_query)
    if result.error_msg:
        raise RuntimeError(
            f"Query error for query:\n{federated_query.nl_query}\n\n"
            f"Error: {result.error_msg}"
        )

    _, kv_results = format_execution_context(
        federated_query=federated_query,
        sparql_results=result.sparql_results,
        sql_results=result.sql_results,
    )

    result_type = federated_query.visualization_type if federated_query else ""
    if not result_type or result_type not in VegaConverter.known_types:
        print("visualization is unknown... defaulting to text")
        result_type = "text"

    kv_results["result_type"] = result_type

    print(f"kv_results={kv_results}")

    rendered = render_telegram_image(
        db=FakeDb(),
        cap_user=FakeUser(),
        telegram_user_id=1,
        telegram_chat_id=1,
        kv_results=kv_results,
        absolute=False,
    )

    if rendered is None:
        print("Skipped: renderer returned None.")
        return

    print(f"Rendered: {rendered['url']}")
    print(f"Bytes: {rendered['bytes']}")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="queries.txt")
    parser.add_argument("--out-dir", default="telegram_render_test_output")
    args = parser.parse_args()

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    os.environ["TELEGRAM_RENDER_DIR"] = str(out_dir)
    os.environ["PUBLIC_BASE_URL"] = "http://localhost:8000"
    os.environ["CAP_LOGO_PATH"] = ""

    content = Path(args.input).read_text(encoding="utf-8")
    queries = extract_query_blocks(content)

    if not queries:
        raise RuntimeError("No SPARQL query queries found.")

    for index, federated_query in enumerate(queries, start=1):
        await render_query_results(
            federated_query=federated_query,
            index=index,
            total=len(queries),
        )

    print(f"\nDone. PNG files written to: {out_dir}")


if __name__ == "__main__":
    asyncio.run(main())
