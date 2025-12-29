"""
Test script for the Natural Language Query Pipeline.
Run this to verify nl components are working correctly.
Not for pytest
"""
import asyncio
import argparse
import json
from pathlib import Path

from cap.util.sparql_util import detect_and_parse_sparql, force_empty_result
from cap.rdf.triplestore import TriplestoreClient
from cap.services.ollama_client import OllamaClient

def _read_content_nl_file(path: str | Path) -> str:
    """Read and return the content of a txt file with nl queries."""
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        return f.read()

class SPARQLGenerationTester:
    """Test SPARQL generation pipeline."""

    def __init__(self, input:str):
        self.input = input
        self.oc = OllamaClient()
        self.tc = TriplestoreClient()

    async def run_all_tests(self):
        if self.input.endswith(".txt"):
            txt_files = [self.input]
        else:
            nl_dir = Path(self.input)
            txt_files = sorted(nl_dir.rglob("*.txt"))

        for txt_file in txt_files:
            print(f"Testing {txt_file}")
            txt_content = _read_content_nl_file(txt_file)
            nl_queries = txt_content.split("\n")
            for query in nl_queries:
                if query.strip() and not query.strip().startswith("#"):
                    res = None
                    try:
                        print(f"Testing {query}")
                        llm_resp = await self.oc.nl_to_sparql(query)
                        is_sequential, sparql_content = detect_and_parse_sparql(llm_resp, query)
                        query_to_validate = force_empty_result(sparql_content)
                        res = await self.tc.execute_query(query_to_validate)

                    except Exception as e:
                        print(f"Test failed!")
                        raw = str(e)

                        try:
                            # Strip HTTP status prefix like "400: "
                            json_start = raw.find("{")
                            if json_start == -1:
                                raise ValueError("No JSON found in exception")

                            payload = json.loads(raw[json_start:])

                            if "exception" in payload:
                                print(f"    exception: {payload['exception']}")

                            if "metadata" in payload:
                                print(f"    metadata: {payload['metadata']}")

                        except Exception as _:
                            print(f"    exception: {raw}")

                        exit()

                    assert "SELECT" in llm_resp, f"Failed with invalid sparql"

                    print(f"✓ Test passed for query\n    {query}")
                    print(f"====GENERATED SPARQL====")
                    print(f"{llm_resp}")
                    print(f"validation: ")
                    print(f"    {res}")
                    print(f"========================")


async def main():
    """Run the test suite."""

    parser = argparse.ArgumentParser(description="Run SPARQL test suite.")
    parser.add_argument(
        "--input",
        default="documentation/examples/nl",
        help="Folder containing .txt text files with nl query examples (default: documentation/examples/nl) or a txt file"
    )
    args = parser.parse_args()

    tester = SPARQLGenerationTester(input=args.input)
    await tester.run_all_tests()

    print("✓✓✓ All tests passed ✓✓✓")


# Usage:
# python sparql_generation_tests.py
# or
# python sparql_generation_tests.py --input path_to_folder_with_txt_files_or_txt_file

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
