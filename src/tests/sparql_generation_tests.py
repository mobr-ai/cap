"""
Test script for the Natural Language Query Pipeline.
Run this to verify nl components are working correctly.
Not for pytest
"""
import asyncio
import argparse
from pathlib import Path
from pprint import pprint

from cap.services.ollama_client import OllamaClient

def _read_content_nl_file(path: str | Path) -> str:
    """Read and return the content of a txt file with nl queries."""
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        return f.read()

class SPARQLGenerationTester:
    """Test SPARQL generation pipeline."""

    def __init__(
        self,
        txt_folder
    ):
        self.txt_folder = txt_folder
        self.oc = OllamaClient()

    async def run_all_tests(self):
        sparql_dir = Path(self.txt_folder)
        txt_files = sorted(sparql_dir.rglob("*.txt"))

        for txt_file in txt_files:
            print(f"Testing {txt_file}")
            txt_content = _read_content_nl_file(txt_file)
            nl_queries = txt_content.split("\n")
            for query in nl_queries:
                if query.strip():
                    try:
                        print(f"Testing {query}")
                        llm_resp = await self.oc.nl_to_sparql(query)

                    except Exception as e:
                        print(f"Test failed!")
                        print(f"    exception: {e}")
                        exit()

                    assert "SELECT" in llm_resp, f"Failed with invalid sparql"

                    print(f"✓ Test passed for query\n    {query}")
                    print(f"====GENERATED SPARQL====")
                    print(f"{llm_resp}")
                    print(f"========================")


async def main():
    """Run the test suite."""

    parser = argparse.ArgumentParser(description="Run SPARQL test suite.")
    parser.add_argument(
        "--txt-folder",
        default="documentation/nl_examples",
        help="Folder containing .txt text files with nl query examples (default: documentation/nl_examples)"
    )
    args = parser.parse_args()

    tester = SPARQLGenerationTester(txt_folder=args.txt_folder)
    await tester.run_all_tests()

    print("✓✓✓ All tests passed ✓✓✓")


# Usage:
# python sparql_tests.py
# or
# python sparql_tests.py http://your-server:8000
# or
# python sparql_tests.py http://your-server:8000 path_to_folder_with_txt_files

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
