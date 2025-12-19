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

from cap.util.nlp_util import lemmatize_text

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.ERROR)

def _read_content_nl_file(path: str | Path) -> str:
    """Read and return the content of a txt file with nl queries."""
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        return f.read()


class LemmatizeTester:
    """Test lemmatization."""


    def __init__(self, base_url: str, input: str):
        self.input: str = input
        self.metrics = []
        self.text_pairs = []

    async def test_text(self, text: str, expected: str = "") -> bool:
        """Test a text lemmatization."""
        try:
            lemmatized = lemmatize_text(text)
            self.text_pairs.append((text, lemmatized))

            if expected:
                assert lemmatized == expected

            return True

        except Exception as e:
            print(f"\nx Text failed: {e}")
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
            print(f"Testing text in file: {txt_file}")
            txt_content = _read_content_nl_file(txt_file)
            nl_queries = txt_content.split("\n")
            for text in nl_queries:
                if text.strip() and not text.strip().startswith("#"):
                    try:
                        result = await self.test_text(text)

                    except Exception as e:
                        print(f"Test failed!")
                        print(f"    exception: {e}")
                        exit()

                    assert result, f"Text failed"

                    print(f"✓ Test passed for text\n    {text}")

        self._print_text_pairs()

    def _print_text_pairs(self):
        """Print all text-normalization pairs, sorted alphabetically."""
        if not self.text_pairs:
            return

        print("\n" + "="*60)
        print("TEXT-LEMMA PAIRS")
        print("="*60)

        # Sort by original text, then by normalized text
        sorted_pairs = sorted(self.text_pairs, key=lambda x: (x[0].lower(), x[1].lower()))

        print(f"\nTotal pairs: {len(sorted_pairs)}\n")
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

    tester = LemmatizeTester(
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

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nTest cancelled by user")
    except Exception as e:
        print(f"\n\nTest suite error: {e}")
