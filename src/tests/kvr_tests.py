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

from cap.util.sparql_result_processor import convert_sparql_to_kv
from cap.services.ollama_client import get_ollama_client, OllamaClient
from cap.services.redis_nl_client import get_redis_nl_client
from cap.services.nl_service import nlq_to_sparql, execute_sparql

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.ERROR)

def _read_content_nl_file(path: str | Path) -> str:
    """Read and return the content of a txt file with nl queries."""
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        return f.read()


class NLQueryTester:
    """Test harness for NL query pipeline."""


    def __init__(self, base_url: str, input: str):
        self.oc = OllamaClient()
        self.input: str = input
        self.base_url: str = base_url.rstrip("/")
        self.metrics = []


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


    @staticmethod
    async def get_kvr_from_query(query:str):
        ollama = get_ollama_client()
        redis_client = get_redis_nl_client()
        _, sparql_query, sparql_queries, is_sequential, sparql_valid = await nlq_to_sparql(
            user_query=query,
            redis_client=redis_client,
            ollama=ollama,
            conversation_history=None
        )

        assert (sparql_valid)
        sparql_dict = await execute_sparql(sparql_query, is_sequential, sparql_queries)
        has_data = sparql_dict["has_data"]
        sparql_results = sparql_dict["sparql_results"]

        if has_data:
            print("sparql_results")
            print(sparql_results)
            kv_results = convert_sparql_to_kv(sparql_results, sparql_query=sparql_query)
            if kv_results:
                return OllamaClient.format_kv(
                    user_query=query,
                    sparql_query=sparql_query,
                    kv_results=kv_results
                )

        return ""


    async def test_query(self, query: str) -> bool:
        """Test a natural language query."""
        start_time = time.time()
        print("\n" + "="*60)
        print(f"Testing Query: {query}")
        print("="*60)

        try:
            resp = await NLQueryTester.get_kvr_from_query(query=query)

            print(resp)
            return True

        except Exception as e:
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
                    try:
                        result = await self.test_query(query)

                    except Exception as e:
                        print(f"Test failed!")
                        print(f"    exception: {e}")
                        exit()

                    assert result, f"Query failed"

                    print(f"✓ Test passed for query\n    {query}")

        self._print_metrics_summary()


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

    tester = NLQueryTester(
        base_url=args.base_url,
        input=args.input
    )
    await tester.run_all_tests()
    print("✓✓✓ All tests passed ✓✓✓")

# Usage:
# python nl_query_tests.py
# or
# python nl_query_tests.py --base-url http://your-server:8000
# or
# python nl_query_tests.py --base-url http://your-server:8000 --input path_to_folder_with_txt_files_or_txt_file

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════╗
    ║  CAP Natural Language Query Pipeline Test Suite  ║
    ╚══════════════════════════════════════════════════╝

    This script will test:
    - Service health check
    - Natural language query processing
    - Result contextualization
    - Streaming response delivery

    Make sure the following are running:
    1. CAP service (python -m cap.main)
    2. Ollama service (ollama serve)
    3. Virtuoso triplestore

    Press Ctrl+C to cancel
    """)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nTest cancelled by user")
    except Exception as e:
        print(f"\n\nTest suite error: {e}")
