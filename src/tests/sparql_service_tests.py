import asyncio
from pathlib import Path

from cap.federated.sparql.sparql_service import execute_sparql
from cap.util.query_file_parser import QueryFileParser

INPUT_FILE = Path("queries.txt")

def extract_query_blocks(content: str) -> list[dict]:
    queries = []
    raw_queries = QueryFileParser.parse(content)
    for nl_query, payload in raw_queries:
        sparql = str(payload.get("sparql", "")).strip()
        if sparql:
            queries.append({
                "nl_query": nl_query,
                "sparql": sparql,
            })

    return queries


def count_rows(sparql_results: dict) -> int:
    bindings = sparql_results.get("results", {}).get("bindings", [])
    if bindings:
        return len(bindings)

    if sparql_results.get("boolean") is not None:
        return 1

    return 0


async def main():
    content = INPUT_FILE.read_text(encoding="utf-8")
    query_blocks = extract_query_blocks(content)

    if not query_blocks:
        raise RuntimeError("No SPARQL queries found in input file.")

    for idx, item in enumerate(query_blocks, start=1):
        nl_query = item["nl_query"]
        sparql = item["sparql"]

        print(f"\n[{idx}/{len(query_blocks)}] Executing NL query:")
        print(nl_query)

        result = await execute_sparql(
            sparql_query=sparql,
            is_sequential=False,
            sparql_queries=[],
        )

        if result.get("error_msg"):
            raise RuntimeError(
                f"SPARQL error while executing query:\n{nl_query}\n\n"
                f"Error: {result['error_msg']}"
            )

        sparql_results = result.get("sparql_results", {})
        row_count = count_rows(sparql_results)

        print(f"Rows returned: {row_count}")

    print("\nAll SPARQL queries executed successfully.")


if __name__ == "__main__":
    asyncio.run(main())
