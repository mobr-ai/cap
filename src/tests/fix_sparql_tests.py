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

from cap.util.sparql_util import _validate_and_fix_sparql
from cap.rdf.cache.sparql_normalizer import SPARQLNormalizer

def _read_content_sparql_file(path: str | Path) -> str:
    """Read and return the content of a SPARQL file."""
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        return f.read()

class SPARQLFixTester:
    """Test SPARQL query pipeline."""

    def __init__(
        self,
        input: str,
    ):
        self.input = input
        self.sn = SPARQLNormalizer()


    async def run_all_tests(self):
        if self.input.endswith(".rq"):
            sparql_files = [self.input]
        else:
            sparql_dir = Path(self.input)
            sparql_files = sorted(sparql_dir.rglob("*.rq"))

        for sparql_file in sparql_files:
            print(f"Testing {sparql_file}")
            query = _read_content_sparql_file(sparql_file)

            try:
                validate, new_query, fixes = _validate_and_fix_sparql(query, "")
                normalized = self.sn.normalize(new_query)

            except Exception as e:
                print(f"Test: {sparql_file} failed!")
                print(f"    query: {query}")
                print(f"    exception: {e}")
                exit()


            print(f"✓ Test passed for {sparql_file})")
            pprint(f"   SPARQL: {query}")
            pprint(f"   Validated to: {new_query}")
            pprint(f"   Normalized to: {normalized}")


async def main():
    """Run the test suite."""

    parser = argparse.ArgumentParser(description="Run SPARQL test suite.")
    parser.add_argument(
        "--input",
        default="documentation/examples/sparql",
        help="Folder containing .rq SPARQL files (default: documentation/examples/sparql)"
    )
    args = parser.parse_args()

    tester = SPARQLFixTester(
        input=args.input,
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
