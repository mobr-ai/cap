"""
Test script for the Natural Language Query Pipeline.
Run this to verify nl components are working correctly.
Not for pytest
"""
import logging
import asyncio
import argparse
import httpx
import json
import time
from pathlib import Path


from cap.services.nl_service import query_with_stream_response
from cap.services.llm_client import LLMClient

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
        self.oc = LLMClient()
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


    async def test_health(self) -> bool:
        """Test if the NL query service is healthy."""
        print("\n" + "="*60)
        print("Testing Health Check")
        print("="*60)

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(f"{self.base_url}/api/v1/nl/health")
                result = response.json()

                print(f"Status: {result.get('status')}")
                print(f"Service: {result.get('service')}")
                print(f"Models: {json.dumps(result.get('models'), indent=2)}")

                return result.get('status') == 'healthy'

            except Exception as e:
                print(f"Health check failed: {e}")
                return False


    async def test_query(self, query: str) -> bool:
        """Test a natural language query."""
        start_time = time.time()
        print("\n" + "="*60)
        print(f"Testing Query: {query}")
        print("="*60)

        try:
            # query_with_stream_response is an async generator, not an HTTP response
            response_generator = query_with_stream_response(
                query=query,
                context="",
                db=None,  # Add your db session if needed
                user=None  # Add your user object if needed
            )

            resp = ""
            async for line in response_generator:
                # Each line is already a string from the generator
                if line.startswith('data: '):
                    data = line[6:].strip()  # Remove 'data: ' prefix and whitespace

                    if data == '[DONE]':
                        execution_time = time.time() - start_time
                        print(resp)
                        print("\n" + "-" * 60)
                        print(f"Execution time: {execution_time:.2f} seconds")
                        print("✓ Query completed successfully")

                        self.metrics.append({
                            'query': query,
                            'execution_time': execution_time,
                            'status': 'success'
                        })

                        return True

                elif line.startswith('status: '):
                    status = line[8:].strip()
                    print(f"\n[{status}]")

                elif line.startswith('Error: '):
                    error = line[7:].strip()
                    print(f"\nx {error}")
                    return False

                elif line.strip() != "":
                    resp = resp + line.rstrip("\n")

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
        print("\n" + "="*60)
        print("NL Query Pipeline Test Suite")
        print("="*60)

        # Test 1: Health check
        health_ok = await self.test_health()
        if not health_ok:
            print("\nHealth check failed. Please ensure:")
            print("  1. llm service is running ")
            print("  2. Models are available")
            print("  3. CAP service is running")
            return

        print("\n✓ Health check passed")

        # Test 2: Simple query
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
    2. llm service
    3. Virtuoso triplestore

    Press Ctrl+C to cancel
    """)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nTest cancelled by user")
    except Exception as e:
        print(f"\n\nTest suite error: {e}")
