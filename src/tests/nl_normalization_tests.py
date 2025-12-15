"""
Test script for the Natural Language Query Pipeline.
Run this to verify nl components are working correctly.
Not for pytest
"""
import logging
import asyncio
import argparse
import time
from pathlib import Path

from cap.services.ollama_client import OllamaClient
from cap.rdf.cache.query_normalizer import QueryNormalizer

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.ERROR)

def _read_content_nl_file(path: str | Path) -> str:
    """Read and return the content of a txt file with nl queries."""
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        return f.read()


class NLNormalizationTester:
    """Test harness for NL normalization pipeline."""


    def __init__(self, base_url: str, input: str):
        self.qn = QueryNormalizer()
        self.input: str = input
        self.metrics = []
        self.query_pairs = []


    def _print_metrics_summary(self):
        """Print summary of all test metrics."""
        if not self.metrics:
            return

        print("\n" + "="*60)
        print("METRICS SUMMARY")
        print("="*60)

        total_queries = len(self.metrics)
        successful = sum(1 for m in self.metrics if m['status'] == 'success')
        failed = total_queries - successful

        execution_times = [m['execution_time'] for m in self.metrics if m['status'] == 'success']

        if execution_times:
            avg_time = sum(execution_times) / len(execution_times)
            min_time = min(execution_times)
            max_time = max(execution_times)

            print(f"\nTotal Queries: {total_queries}")
            print(f"Successful: {successful}")
            print(f"Failed: {failed}")
            print(f"\nExecution Times:")
            print(f"  Average: {avg_time:.2f}s")
            print(f"  Minimum: {min_time:.2f}s")
            print(f"  Maximum: {max_time:.2f}s")
            print(f"  Total: {sum(execution_times):.2f}s")

            print(f"\nPer-Query Breakdown:")
            for m in self.metrics:
                status_icon = "v" if m['status'] == 'success' else "x"
                print(f"  {status_icon} {m['execution_time']:.2f}s - {m['query'][:60]}...")


    async def test_query(self, query: str, expected: str = "") -> bool:
        """Test a natural language query."""
        start_time = time.time()
        print("\n" + "="*60)
        print(f"Testing normalization of: {query}")
        print("="*60)

#        try:
        normalized = QueryNormalizer.normalize(query)
        # Store the query pair
        self.query_pairs.append((query, normalized))

        if expected:
            assert normalized == expected

        query_category = OllamaClient._categorize_query(query, "multiple")
        print(f"{query} normalizes to '{normalized}' on category {query_category}")
        return True

#        except Exception as e:
        execution_time = time.time() - start_time
        print(f"\nx Query failed: {e}")
        print(f"Time before failure: {execution_time:.2f} seconds")

        self.metrics.append({
            'query': query,
            'execution_time': execution_time,
            'status': 'failed',
            'error': str(e)
        })

        return False


    async def run_all_tests(self):
        """Run all tests."""
        print("\n" + "="*60)
        print("NL Query Normalization Pipeline Test Suite")
        print("="*60)

        if self.input.endswith(".txt"):
            txt_files = [self.input]
        else:
            nl_dir = Path(self.input)
            txt_files = sorted(nl_dir.rglob("*.txt"))

        for txt_file in txt_files:
            print(f"Testing queries in file: {txt_file}")
            txt_content = _read_content_nl_file(txt_file)
            nl_queries = txt_content.split("\n")
            for query in nl_queries:
                if query.strip() and not query.strip().startswith("#"):
                    result = await self.test_query(query)
                    assert result, f"Query failed"

                    print(f"✓ Test passed for query\n    {query}")

        self._print_metrics_summary()
        self._print_query_pairs()

    def _print_query_pairs(self):
        """Print all query-normalization pairs, sorted alphabetically."""
        if not self.query_pairs:
            return

        print("\n" + "="*60)
        print("QUERY NORMALIZATION PAIRS")
        print("="*60)

        # Sort by original query, then by normalized query
        sorted_pairs = sorted(self.query_pairs, key=lambda x: (x[0].lower(), x[1].lower()))

        print(f"\n Pairs by query: {len(sorted_pairs)}\n")
        for original, normalized in sorted_pairs:
            print(f"'{original}' -> '{normalized}'")

        # Sort by normalized query, then by original query
        sorted_pairs = sorted(self.query_pairs, key=lambda x: (x[1].lower(), x[0].lower()))
        print(f"\n\n Pairs by normalization: {len(sorted_pairs)}\n")
        for original, normalized in sorted_pairs:
            print(f"'{original}' -> '{normalized}'")

async def main():
    """Run the test suite."""

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run SPARQL test suite.")
    parser.add_argument(
        "--base-url",
        default="http://localhost:8000",
        help="Base URL of the NL endpoint (default: http://localhost:8000)"
    )
    parser.add_argument(
        "--input",
        default="documentation/examples/nl",
        help="Folder containing .txt files with NL queries (default: documentation/examples/nl) or a txt file"
    )
    args = parser.parse_args()

    tester = NLNormalizationTester(
        base_url=args.base_url,
        input=args.input
    )
    await tester.run_all_tests()
    print("✓✓✓ All tests passed ✓✓✓")

# Usage:
# python nl_normalization_tests.py
# or
# python nl_normalization_tests.py --base-url http://your-server:8000
# or
# python nl_normalization_tests.py --base-url http://your-server:8000 --input path_to_folder_with_txt_files_or_txt_file

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════╗
    ║  CAP NP Query Normalization Pipeline Test Suite  ║
    ╚══════════════════════════════════════════════════╝

    Press Ctrl+C to cancel
    """)

    asyncio.run(main())
