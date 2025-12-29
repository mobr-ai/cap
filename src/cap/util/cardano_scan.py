import re
from typing import Any

# Cardanoscan base URLs
CARDANOSCAN_BASE = "https://cardanoscan.io"

# Property to entity type mapping from ontology
PROPERTY_TO_ENTITY = {
    # Block properties
    'hasHash': 'block',  # b:Block hasHash
    'hasPreviousBlock': 'block',

    # Transaction properties
    'hasTxID': 'transaction',  # b:Transaction hasTxID

    # Epoch properties
    'hasEpochNumber': 'epoch',  # c:Epoch hasEpochNumber

    # Address properties
    'hasAddressId': 'address',  # c:TransactionOutput hasAddressId

    # Pool properties
    'hasDelegatee': 'pool',  # c:Delegation hasDelegatee

    # Policy properties
    'hasPolicyId': 'policy',  # c:NFT, c:MultiAssetCNT hasPolicyId
}

def _detect_entity_from_ontology(var_name: str, sparql_query: str) -> str | None:
    """
    Detect entity type by analyzing SPARQL query for ontology property usage.

    Args:
        var_name: Variable name from SPARQL results
        sparql_query: The SPARQL query to analyze

    Returns:
        Entity type or None
    """
    if not sparql_query:
        return None

    query_text = sparql_query
    if isinstance(sparql_query, list):
        query_text = " ".join([q.get('query', '') if isinstance(q, dict) else str(q) for q in sparql_query])
    elif isinstance(sparql_query, dict):
        query_text = sparql_query.get('query', str(sparql_query))

    # Search for property patterns with this variable
    for property_name, entity_type in PROPERTY_TO_ENTITY.items():
        # Look for patterns like: ?something property ?var_name
        patterns = [
            rf'{property_name}\s+\?{var_name}\b',
            rf'b:{property_name}\s+\?{var_name}\b',
            rf'c:{property_name}\s+\?{var_name}\b',
        ]

        for pattern in patterns:
            if re.search(pattern, query_text, re.IGNORECASE):
                return entity_type

    return None


def convert_entity_to_cardanoscan_link(var_name: str, value: Any, sparql_query: str = "") -> str:
    """
    Convert blockchain entities to Cardanoscan links using ontology mappings.

    Args:
        var_name: Variable name from SPARQL query
        value: The value to convert
        sparql_query: The SPARQL query for ontology analysis

    Returns:
        HTML link string if entity detected, original value otherwise
    """
    if not isinstance(value, str):
        return str(value)

    value_clean = value.strip()

    # Detect entity type from ontology property usage
    entity_type = _detect_entity_from_ontology(var_name, sparql_query)

    if not entity_type:
        return value

    # Build Cardanoscan URL based on entity type
    url_map = {
        'transaction': f"{CARDANOSCAN_BASE}/transaction/{value_clean}",
        'block': f"{CARDANOSCAN_BASE}/block/{value_clean}",
        'epoch': f"{CARDANOSCAN_BASE}/epoch/{value_clean}",
        'address': f"{CARDANOSCAN_BASE}/address/{value_clean}",
        'pool': f"{CARDANOSCAN_BASE}/pool/{value_clean}",
        'policy': f"{CARDANOSCAN_BASE}/tokenPolicy/{value_clean}",
        'metadata': f"{CARDANOSCAN_BASE}/transaction/{value_clean}#metadata",
    }

    url = url_map.get(entity_type)
    if not url:
        return value

    # Abbreviate long hashes for display
    display_value = value_clean
    if entity_type in ['transaction', 'block', 'metadata', 'policy']:
        if len(value_clean) > 19:
            display_value = f"{value_clean[:8]}...{value_clean[-8:]}"

    return f'<a href="{url}" target="_blank" title="{value_clean}">{display_value}</a>'
