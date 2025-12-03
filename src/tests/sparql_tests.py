"""
Test script for the Natural Language Query Pipeline.
Run this to verify nl components are working correctly.
Not for pytest
"""
import asyncio
import argparse
import httpx
from pathlib import Path
from pprint import pprint

from cap.rdf.triplestore import TriplestoreClient

def _read_content_sparql_file(path: str | Path) -> str:
    """Read and return the content of a SPARQL file."""
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        return f.read()

class SPARQLQueryTester:
    """Test SPARQL query pipeline."""

    def __init__(
        self,
        base_url: str,
        sparql_folder,
        use_api: bool = False
    ):
        self.base_url = base_url.rstrip("/")
        self.sparql_folder = sparql_folder
        self.use_api = use_api
        self.tc = TriplestoreClient() if not use_api else None
        self._http_client = None

    async def _get_http_client(self):
        if not self._http_client:
            timeout = httpx.Timeout(300.0, connect=10.0)
            self._http_client = httpx.AsyncClient(
                timeout=timeout,
                limits=httpx.Limits(
                    max_keepalive_connections=20,
                    max_connections=50,
                    keepalive_expiry=300.0
                ),
                http2=True  # Enable HTTP/2 for better performance
            )
        return self._http_client

    async def _close(self):
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    async def execute(self, query: str):
        if not self.use_api:
            return await self.tc.execute_query(query)

        client = await self._get_http_client()
        resp = await client.post(
            f"{self.base_url}/api/v1/query",
            json={"query": query, "type": "SELECT"}
        )

        resp.raise_for_status()
        await self._close()
        return resp.json()

    async def run_all_tests(self):
        sparql_dir = Path(self.sparql_folder)
        sparql_files = sorted(sparql_dir.rglob("*.rq"))

        for sparql_file in sparql_files:
            print(f"Testing {sparql_file}")
            query = _read_content_sparql_file(sparql_file)

            try:
                resp = await self.execute(query)

                # API returns:
                #   {"results": {...}}
                # Virtuoso returns:
                #   {"results": {...}}
                bindings = resp.get("results", {})

            except Exception as e:
                print(f"Test: {sparql_file} failed!")
                print(f"    query: {query}")
                print(f"    exception: {e}")
                exit()

            assert bindings is not None, f"Bindings is None for {sparql_file}"
            assert len(bindings) > 0, f"No results returned for {sparql_file}"

            print(f"✓ Test passed for {sparql_file} ({len(bindings)} bindings)")
            pprint(f"   SPARQL: {query}")
            pprint(f"   Bindings: {bindings}")


async def main():
    """Run the test suite."""

    parser = argparse.ArgumentParser(description="Run SPARQL test suite.")
    parser.add_argument(
        "--base-url",
        default="http://localhost:8000",
        help="Base URL of the SPARQL endpoint (default: http://localhost:8000)"
    )
    parser.add_argument(
        "--sparql-folder",
        default="documentation/examples/sparql",
        help="Folder containing .rq SPARQL files (default: documentation/examples/sparql)"
    )
    parser.add_argument(
        "--use-api",
        action="store_true",
        help="Use CAP API instead of direct triplestore connection"
    )
    args = parser.parse_args()

    tester = SPARQLQueryTester(
        base_url=args.base_url,
        sparql_folder=args.sparql_folder,
        use_api=args.use_api
    )

    await tester.run_all_tests()

    print("✓✓✓ All tests passed ✓✓✓")


# Usage:
# python sparql_tests.py
# or
# python sparql_tests.py http://your-server:8000
# or
# python sparql_tests.py http://your-server:8000 path_to_folder_with_sparql_files

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════╗
    ║    CAP SPARQL Query Pipeline Test Suite  ║
    ╚══════════════════════════════════════════╝

    Make sure the following are running:
    1. CAP service (python -m cap.main)
    2. Virtuoso triplestore

    Press Ctrl+C to cancel
    """)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nTest cancelled by user")
    except Exception as e:
        print(f"\n\nTest suite error: {e}")
