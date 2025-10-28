"""
Test script for the Natural Language Query Pipeline.
Run this to verify nl components are working correctly.
Not for pytest
"""
import asyncio
import httpx
import json

class NLQueryTester:
    """Test harness for NL query pipeline."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip("/")

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
                print(f"❌ Health check failed: {e}")
                return False

    async def test_query(self, query: str) -> bool:
        """Test a natural language query."""
        print("\n" + "="*60)
        print(f"Testing Query: {query}")
        print("="*60)

        async with httpx.AsyncClient(timeout=300.0) as client:
            try:
                response = await client.post(
                    f"{self.base_url}/api/v1/nl/query",
                    json={"query": query, "temperature": 0.1},
                    headers={"Accept": "text/event-stream"}
                )

                if response.status_code != 200:
                    print(f"❌ HTTP Error: {response.status_code}")
                    print(f"Response: {response.text}")
                    return False

                print("\nStreaming Response:")
                print("-" * 60)

                buffer = ""
                async for chunk in response.aiter_bytes():
                    buffer += chunk.decode('utf-8')

                    while '\n' in buffer:
                        line, buffer = buffer.split('\n', 1)

                        if line.startswith('data: '):
                            data = line[6:]  # Remove 'data: ' prefix

                            if data == '[DONE]':
                                print("\n" + "-" * 60)
                                print("✅ Query completed successfully")
                                return True

                            # Print the data chunk
                            print(data, end='', flush=True)

                return True

            except Exception as e:
                print(f"❌ Query failed: {e}")
                return False

    async def run_all_tests(self):
        """Run all tests."""
        print("\n" + "="*60)
        print("NL Query Pipeline Test Suite")
        print("="*60)

        # Test 1: Health check
        health_ok = await self.test_health()
        if not health_ok:
            print("\n❌ Health check failed. Please ensure:")
            print("  1. Ollama service is running (ollama serve)")
            print("  2. Models are available (ollama list)")
            print("  3. CAP service is running")
            return

        print("\n✅ Health check passed")

        # Test 2: Simple query
        test_queries = [
            "What is the current epoch?",
            "Show me the latest 5 blocks",
            "List the top 3 stake pools",
        ]

        results = []
        for query in test_queries:
            result = await self.test_query(query)
            results.append((query, result))
            await asyncio.sleep(1)  # Brief pause between queries

        # Summary
        print("\n" + "="*60)
        print("Test Summary")
        print("="*60)

        for query, success in results:
            status = "✅" if success else "❌"
            print(f"{status} {query}")

        total_passed = sum(1 for _, success in results if success)
        print(f"\nPassed: {total_passed}/{len(results)}")


async def main():
    """Run the test suite."""
    import sys

    # Parse command line arguments
    base_url = "http://localhost:8000"
    if len(sys.argv) > 1:
        base_url = sys.argv[1]

    tester = NLQueryTester(base_url)
    await tester.run_all_tests()


if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════╗
    ║  CAP Natural Language Query Pipeline Test Suite  ║
    ╚══════════════════════════════════════════════════╝

    This script will test:
    - Service health check
    - Natural language query processing
    - SPARQL generation and execution
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


# Additional utility functions for manual testing

async def simple_query_test(query: str, base_url: str = "http://localhost:8000"):
    """Quick test of a single query."""
    async with httpx.AsyncClient(timeout=300.0) as client:
        response = await client.post(
            f"{base_url}/api/v1/nl/query",
            json={"query": query, "temperature": 0.1},
            headers={"Accept": "text/event-stream"}
        )

        async for chunk in response.aiter_text():
            print(chunk, end='', flush=True)


# Usage:
# python test_nl_query_pipeline.py
# or
# python test_nl_query_pipeline.py http://your-server:8000