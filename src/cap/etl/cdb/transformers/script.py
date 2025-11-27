import logging
from typing import Any

from cap.etl.cdb.transformers.transformer import BaseTransformer

logger = logging.getLogger(__name__)

class ScriptTransformer(BaseTransformer):
    """Transforms script data to RDF aligned with Cardano ontology."""

    def transform(self, scripts: list[dict[str, Any]]) -> str:
        """Transform scripts to RDF Turtle format."""
        turtle_lines = []

        for script in scripts:
            script_uri = self.create_uri('script', script['hash'])

            # Determine script type
            script_class = 'c:PlutusScript' if 'plutus' in script['type'].lower() else 'c:NativeScript'

            turtle_lines.append(f"{script_uri} a {script_class} ;")

            if script['hash']:
                turtle_lines.append(f"    b:hasHash \"{script['hash']}\" ;")

            if script['tx_hash']:
                tx_uri = self.create_transaction_uri(script['tx_hash'])
                turtle_lines.append(f"    c:embeddedIn {tx_uri} ;")

            # Remove trailing semicolon and add period
            if turtle_lines and turtle_lines[-1].endswith(' ;'):
                turtle_lines[-1] = turtle_lines[-1][:-2] + ' .'

            turtle_lines.append("")

            # Create SmartContract instance for Plutus scripts
            if 'plutus' in script['type'].lower():
                contract_uri = self.create_uri('smart_contract', script['hash'])
                turtle_lines.append(f"{contract_uri} a b:SmartContract ;")
                turtle_lines.append(f"    c:hasScript {script_uri} ;")

                if script['tx_hash']:
                    tx_uri = self.create_transaction_uri(script['tx_hash'])
                    turtle_lines.append(f"    c:embeddedIn {tx_uri} ;")

                # Add script address if available
                if script.get('hash'):
                    turtle_lines.append(f"    c:hasScriptAddress \"{script['hash']}\" ;")

                # Remove trailing semicolon and add period
                if turtle_lines[-1].endswith(' ;'):
                    turtle_lines[-1] = turtle_lines[-1][:-2] + ' .'

                turtle_lines.append("")

        return '\n'.join(turtle_lines)