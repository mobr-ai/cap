"""
Ollama client tests
"""
import logging
import json
import sys
import asyncio

from cap.services.ollama_client import OllamaClient
from cap.util.sparql_util import _clean_sparql

async def test_health_check():
    """Test 1: Health Check - Verify Ollama service is running."""
    print("\n" + "="*70)
    print("TEST 1: Health Check")
    print("="*70)
    print("Purpose: Verify that Ollama service is accessible and responding\n")

    client = OllamaClient()

    try:
        is_healthy = await client.health_check()

        if is_healthy:
            print("SUCCESS: Ollama service is healthy and responding")
            print(f"   Base URL: {client.base_url}")
            print(f"   SPARQL Model: {client.llm_model}")
        else:
            print("FAILED: Ollama service is not responding")
            print("   Please ensure 'ollama serve' is running")

        return is_healthy

    except Exception as e:
        print(f"ERROR: {e}")
        return False

    finally:
        await client.close()

async def test_generate_complete():
    """Test 2: Generate Complete - Non-streaming text generation."""
    print("\n" + "="*70)
    print("TEST 2: Generate Complete (Non-Streaming)")
    print("="*70)
    print("Purpose: Generate a complete response without streaming\n")

    client = OllamaClient()

    try:
        prompt = "Explain what a blockchain is in one sentence."
        system_prompt = "You are a helpful assistant. Be concise."

        print(f"Prompt: '{prompt}'")
        print(f"System: '{system_prompt}'")
        print("\nGenerating response...\n")

        response = await client.generate_complete(
            prompt=prompt,
            model=client.llm_model,  # Using base model for demo
            system_prompt=system_prompt,
            temperature=0.3
        )

        print("Response received:")
        print("-" * 70)
        print(response)
        print("-" * 70)

    except Exception as e:
        print(f"ERROR: {e}")
        raise

    finally:
        await client.close()

async def test_generate_stream():
    """Test 3: Generate Stream - Streaming text generation."""
    print("\n" + "="*70)
    print("TEST 3: Generate Stream (Streaming)")
    print("="*70)
    print("Purpose: Generate a response with streaming for real-time display\n")

    client = OllamaClient()

    try:
        prompt = "Write a haiku about blockchain technology."
        system_prompt = "You are a creative poet."

        print(f"Prompt: '{prompt}'")
        print(f"System: '{system_prompt}'")
        print("\nStreaming response:\n")
        print("-" * 70)

        async for chunk in client.generate_stream(
            prompt=prompt,
            model=client.llm_model,
            system_prompt=system_prompt,
            temperature=0.7
        ):
            print(chunk, end='', flush=True)

        print("\n" + "-" * 70)
        print("Stream completed successfully")

    except Exception as e:
        print(f"\nERROR: {e}")
        raise

    finally:
        await client.close()

async def test_nl_to_sparql():
    """Test 4: NL to SPARQL - Convert natural language to SPARQL query."""
    print("\n" + "="*70)
    print("TEST 4: Natural Language to SPARQL")
    print("="*70)
    print("Purpose: Convert a natural language question to SPARQL query\n")

    client = OllamaClient()

    try:
        natural_query = "Show me the latest 5 blocks"
        print(f"Natural Language Query: '{natural_query}'")
        print("\nGenerating SPARQL...\n")

        sparql_query = await client.nl_to_sparql(
            natural_query=natural_query,
            conversation_history=""
        )

        print("Generated SPARQL Query:")
        print("-" * 70)
        print(sparql_query)
        print("-" * 70)
        print("\nNote: The query has been automatically cleaned of markdown")
        print("      and explanatory text")

    except Exception as e:
        print(f"ERROR: {e}")
        raise

    finally:
        await client.close()

async def test_contextualize_answer():
    """Test 5: Contextualize Answer - Generate natural language answer from results."""
    print("\n" + "="*70)
    print("TEST 5: Contextualize Answer")
    print("="*70)
    print("Purpose: Generate a natural language answer from SPARQL results\n")

    client = OllamaClient()

    try:
        user_query = "What is the current epoch?"

        sparql_query = """
PREFIX c: <https://mobr.ai/ont/cardano#>
SELECT ?epoch ?epochNo ?startTime
WHERE {
?epoch a c:Epoch ;
        c:hasEpochNumber ?epochNo ;
        c:hasStartTime ?startTime .
}
ORDER BY DESC(?epochNo)
LIMIT 1
"""

        # Mock SPARQL results
        sparql_results = {
            "results": {
                "bindings": [
                    {
                        "epoch": {"value": "https://mobr.ai/ont/cardano#epoch/450"},
                        "epochNo": {"value": "450"},
                        "startTime": {"value": "2024-01-15T00:00:00Z"}
                    }
                ]
            }
        }

        system_prompt = """You are a helpful assistant explaining Cardano blockchain data.
Be conversational and clear. Format dates nicely."""

        print(f"User Query: '{user_query}'")
        print(f"\nSPARQL Query:\n{sparql_query}")
        print(f"\nResults:\n{json.dumps(sparql_results, indent=2)}")
        print("\nGenerating contextualized answer:\n")
        print("-" * 70)

        async for chunk in client.generate_answer_with_context(
            user_query=user_query,
            sparql_query=sparql_query,
            sparql_results=sparql_results,
            kv_results={},
            system_prompt=system_prompt
        ):
            print(chunk, end='', flush=True)

        print("\n" + "-" * 70)
        print("Contextualized answer completed")

    except Exception as e:
        print(f"\nERROR: {e}")
        raise

    finally:
        await client.close()

async def test_clean_sparql():
    """Test 6: Clean SPARQL - Demonstrate SPARQL cleaning logic."""
    print("\n" + "="*70)
    print("TEST 6: SPARQL Cleaning")
    print("="*70)
    print("Purpose: Show how raw LLM output is cleaned to extract pure SPARQL\n")

    client = OllamaClient()

    # Example messy SPARQL responses from LLM
    test_cases = [
        {
            "name": "With markdown and explanation",
            "input": """Here is the SPARQL query:

```sparql
PREFIX b: <https://mobr.ai/ont/blockchain#>
SELECT ?block ?hash
WHERE {
?block a b:Block ;
        b:hasHash ?hash .
}
LIMIT 5
```

This query will return the latest 5 blocks."""
        },
        {
            "name": "With explanatory text",
            "input": """The query is:
PREFIX c: <https://mobr.ai/ont/cardano#>
SELECT ?epoch
WHERE {
?epoch a c:Epoch .
}

This will get all epochs."""
        },
        {
            "name": "Clean query (no cleaning needed)",
            "input": """PREFIX b: <https://mobr.ai/ont/blockchain#>
SELECT ?tx ?hash
WHERE {
?tx a b:Transaction ;
    b:hasHash ?hash .
}
LIMIT 10"""
        }
    ]

    for i, test_case in enumerate(test_cases, 1):
        print(f"\nTest Case {i}: {test_case['name']}")
        print("-" * 70)
        print("INPUT:")
        print(test_case['input'])
        print("\nOUTPUT (cleaned):")
        cleaned = _clean_sparql(test_case['input'])
        print(cleaned)
        print("-" * 70)

    print("\nSPARQL cleaning demonstration completed")

    await client.close()

async def test_full_pipeline():
    """Test 7: Full Pipeline - End-to-end example."""
    print("\n" + "="*70)
    print("TEST 7: Full Pipeline Simulation")
    print("="*70)
    print("Purpose: Simulate the complete NL query pipeline\n")

    client = OllamaClient()

    try:
        # Step 1: User asks a question
        user_question = "How many blocks are in the latest epoch?"
        print(f"👤 User Question: '{user_question}'")

        # Step 2: Convert to SPARQL
        print("\n🔧 Step 1: Converting to SPARQL...")
        # To test, using mock SPARQL instead of calling the model
        mock_sparql = """
PREFIX c: <https://mobr.ai/ont/cardano#>
PREFIX b: <https://mobr.ai/ont/blockchain#>
SELECT (COUNT(?block) as ?count)
WHERE {
?block a b:Block ;
        c:hasEpoch ?epoch .
?epoch c:hasEpochNumber ?epochNo .
}
GROUP BY ?epochNo
ORDER BY DESC(?epochNo)
LIMIT 1
"""
        print(f"   Generated SPARQL:\n{mock_sparql}")

        # Step 3: Execute SPARQL (mock results)
        print("\nStep 2: Executing SPARQL query...")
        mock_results = {
            "results": {
                "bindings": [
                    {"count": {"value": "21600"}}
                ]
            }
        }
        print(f"   Results: {json.dumps(mock_results, indent=2)}")

        # Step 4: Contextualize answer
        print("\nStep 3: Generating contextualized answer...\n")
        print("-" * 70)

        answer_system_prompt = "Explain Cardano data clearly and conversationally."

        async for chunk in client.generate_answer_with_context(
            user_query=user_question,
            sparql_query=mock_sparql,
            sparql_results=mock_results,
            kv_results={},
            system_prompt=answer_system_prompt
        ):
            print(chunk, end='', flush=True)

        print("\n" + "-" * 70)
        print("\nFull pipeline completed successfully!")

    except Exception as e:
        print(f"\nERROR: {e}")
        raise

    finally:
        await client.close()

async def safe_run(test_func):
    try:
        return await test_func()
    except Exception as e:
        print(f"\nTest failed: {test_func.__name__}\nError: {e}\nStopping further execution.")
        sys.exit(1)

async def run_all_tests():
    """Run all illustrative tests."""
    print("\nStarting OllamaClient test suite...\n")

    is_healthy = await safe_run(test_health_check)
    if not is_healthy:
        print("\n" + "="*70)
        print("WARNING: Ollama service is not available")
        print("="*70)
        print("\nPlease start Ollama service:")
        print("  $ ollama serve")
        print("\nThen run this test again.")
        return

    await safe_run(test_generate_complete)
    await safe_run(test_generate_stream)
    await safe_run(test_nl_to_sparql)
    await safe_run(test_contextualize_answer)
    await safe_run(test_clean_sparql)
    await safe_run(test_full_pipeline)

    print("\n" + "="*70)
    print("All tests completed!")
    print("="*70)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    print("""
    OllamaClient tests
      Prerequisites:
        1. Ollama service must be running (ollama serve)
        2. OllamaClient model must be available
    """)

    # Run all tests
    try:
        asyncio.run(run_all_tests())
    except KeyboardInterrupt:
        print("\n\nOC tests interrupted by user")
    except Exception as e:
        print(f"\n\nOC tests error: {e}")
        import traceback
        traceback.print_exc()